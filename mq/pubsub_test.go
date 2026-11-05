package mq

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testProject = "test-project"
	testTopic   = "sso-updates"
)

func createTopic(t *testing.T, client *pubsub.Client, topicID string) {
	t.Helper()

	_, err := client.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{
		Name: "projects/" + testProject + "/topics/" + topicID,
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A fake Pub/Sub server with testTopic created, and a broker publishing to topicID on it
func newFakePubSub(t *testing.T, topicID string, opts ...pstest.ServerReactorOption) (*pstest.Server, *pubsubBroker) {
	t.Helper()

	server := pstest.NewServer(opts...)
	t.Cleanup(func() { _ = server.Close() })

	conn, err := grpc.NewClient(server.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	config := Config{PubSubProjectID: testProject, PubSubTopicID: topicID}

	publisher, err := newPubSubBroker(context.Background(), config, logrus.StandardLogger(), option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}

	createTopic(t, publisher.client, testTopic)

	return server, publisher
}

func newTestEvent(t *testing.T, eventType EventType, userID int64, payload any) Event {
	t.Helper()

	event, err := NewEvent(eventType, userID, payload)
	if err != nil {
		t.Fatal(err)
	}

	event.Source = testSource

	return event
}

func TestPubSubPublishesTheEvent(t *testing.T) {
	server, publisher := newFakePubSub(t, testTopic)
	event := newTestEvent(t, EmailUpdated, 123456, map[string]any{"id": 123456, "email": "a@b.pl"})

	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}

	messages := server.Messages()
	if len(messages) != 1 {
		t.Fatalf("%d messages published, expected 1", len(messages))
	}

	message := messages[0]
	if string(message.Data) != string(event.Body) {
		t.Errorf("data %s, expected the event body %s", message.Data, event.Body)
	}

	if message.OrderingKey != "123456" {
		t.Errorf("ordering key %q, expected the user ID", message.OrderingKey)
	}

	expected := event.Attributes()
	if len(message.Attributes) != len(expected) {
		t.Errorf("attributes %v, expected %v", message.Attributes, expected)
	}

	for name, value := range expected {
		if message.Attributes[name] != value {
			t.Errorf("attribute %s is %q, expected %q", name, message.Attributes[name], value)
		}
	}
}

func TestPubSubKeepsTheOrderOfTheEventsOfAUser(t *testing.T) {
	server, publisher := newFakePubSub(t, testTopic)

	var published []string

	for _, userID := range []int64{7, 8, 7, 7} {
		event := newTestEvent(t, PersonalDataUpdated, userID, map[string]int64{"id": userID})
		if err := publisher.Publish(context.Background(), event); err != nil {
			t.Fatal(err)
		}

		if userID == 7 {
			published = append(published, event.ID)
		}
	}

	var received []string

	for _, message := range server.Messages() {
		if message.OrderingKey == "7" {
			received = append(received, message.Attributes["event_id"])
		}
	}

	if strings.Join(received, ",") != strings.Join(published, ",") {
		t.Errorf("events of user 7 arrived as %v, published as %v", received, published)
	}
}

// a client hanging up after the data has been changed must not cancel the broadcast
func TestPubSubPublishesWhenTheRequestIsCancelled(t *testing.T) {
	server, publisher := newFakePubSub(t, testTopic)
	SetForTest(t, publisher)

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Broadcast(requestCtx, DeleteUser, 1, map[string]any{"id": 1, "type": "delete_user"}); err != nil {
		t.Fatal(err)
	}

	if messages := server.Messages(); len(messages) != 1 {
		t.Errorf("%d messages published, expected 1", len(messages))
	}
}

func TestPubSubReportsAMissingTopic(t *testing.T) {
	_, publisher := newFakePubSub(t, "missing")

	if err := publisher.Publish(context.Background(), newTestEvent(t, DeleteUser, 1, struct{}{})); err == nil {
		t.Error("publishing to a topic that does not exist succeeded")
	}
}

// after a failure the ordering key is paused - the next event of the user must not fail because of it
func TestPubSubResumesTheOrderingKeyAfterAFailure(t *testing.T) {
	_, publisher := newFakePubSub(t, testTopic, pstest.WithErrorInjection("Publish", codes.PermissionDenied, "denied"))

	for attempt := 1; attempt <= 2; attempt++ {
		err := publisher.Publish(context.Background(), newTestEvent(t, EmailUpdated, 1, struct{}{}))
		if err == nil {
			t.Fatalf("attempt %d succeeded despite the injected error", attempt)
		}

		if !strings.Contains(err.Error(), "denied") || strings.Contains(err.Error(), "paused") {
			t.Errorf("attempt %d failed with %q, expected the injected error", attempt, err)
		}
	}
}

func TestPubSubCloseStopsPublishing(t *testing.T) {
	_, publisher := newFakePubSub(t, testTopic)

	if err := publisher.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := publisher.Publish(context.Background(), newTestEvent(t, DeleteUser, 1, struct{}{})); err == nil {
		t.Error("publishing after Close succeeded")
	}
}

// the local and CI setup: configured from the environment, connected to an emulator, no credentials
func TestPubSubFromTheEnvironmentWithAnEmulator(t *testing.T) {
	server := pstest.NewServer()
	t.Cleanup(func() { _ = server.Close() })

	t.Setenv("PUBSUB_EMULATOR_HOST", server.Addr)
	t.Setenv("PUBSUB_PROJECT_ID", testProject)
	t.Setenv("PUBSUB_TOPIC_ID", testTopic)

	admin, err := pubsub.NewClient(context.Background(), testProject)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = admin.Close() })
	createTopic(t, admin, testTopic)

	if err := Init(context.Background(), ConfigFromEnv(testSource, nil)); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = Close(context.Background()) })

	payload := map[string]any{"id": 42, "force_logout": true}
	if err := Broadcast(context.Background(), ForceLogout, 42, payload); err != nil {
		t.Fatal(err)
	}

	messages := server.Messages()
	if len(messages) != 1 || string(messages[0].Data) != `{"force_logout":true,"id":42}` {
		t.Errorf("unexpected messages %+v", messages)
	}
}
