package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

const testSource = "sso"

type publisherFunc func(ctx context.Context, event Event) error

func (f publisherFunc) Publish(ctx context.Context, event Event) error {
	return f(ctx, event)
}

func TestBroadcastWithoutInitIsDisabled(t *testing.T) {
	if current.Load() != nil {
		t.Fatal("broadcasting is set up before the test")
	}

	if err := Broadcast(context.Background(), DeleteUser, 1, struct{}{}); err != nil {
		t.Errorf("a disabled broadcast failed: %v", err)
	}
}

func TestInitRequiresRabbitMQDSN(t *testing.T) {
	if err := Init(context.Background(), Config{Source: testSource}); err == nil {
		t.Fatal("Init accepted a configuration without RABBITMQ_DSN")
	}

	if current.Load() != nil {
		t.Error("a failed Init enabled broadcasting")
	}
}

func TestInitAndClose(t *testing.T) {
	// no connection is made until the first broadcast
	if err := Init(context.Background(), Config{Source: testSource, RabbitMQDSN: "amqp://localhost:5672/"}); err != nil {
		t.Fatal(err)
	}

	if current.Load() == nil {
		t.Fatal("Init did not enable broadcasting")
	}

	if err := Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if current.Load() != nil {
		t.Error("Close did not disable broadcasting")
	}
}

func TestBroadcastPublishesTheEvent(t *testing.T) {
	recorder := &Recorder{}
	SetForTest(t, recorder)

	if err := Broadcast(context.Background(), PersonalDataUpdated, 42, map[string]any{"id": 42}); err != nil {
		t.Fatal(err)
	}

	events := recorder.Events()
	if len(events) != 1 {
		t.Fatalf("%d events published, expected 1", len(events))
	}

	event := events[0]
	if event.Type != PersonalDataUpdated || event.UserID != 42 || event.Source != "test" ||
		string(event.Body) != `{"id":42}` || event.ID == "" || event.OccurredAt.IsZero() {
		t.Errorf("unexpected event %+v", event)
	}
}

func TestBroadcastReturnsPublishingErrors(t *testing.T) {
	brokerDown := errors.New("broker down")
	recorder := &Recorder{}
	recorder.FailOn(DeleteUser, brokerDown)
	SetForTest(t, recorder)

	if err := Broadcast(context.Background(), DeleteUser, 1, struct{}{}); !errors.Is(err, brokerDown) {
		t.Errorf("Broadcast returned %v, expected the publishing error", err)
	}
}

// a client hanging up after the data has been changed must not cancel the broadcast
func TestBroadcastOutlivesTheRequestContext(t *testing.T) {
	SetForTest(t, publisherFunc(func(ctx context.Context, _ Event) error {
		if ctx.Err() != nil {
			t.Error("the broadcast inherited the cancellation of the request")
		}

		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > publishWaitTimeout {
			t.Errorf("the broadcast is not bound to %s", publishWaitTimeout)
		}

		return nil
	}))

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Broadcast(requestCtx, EmailUpdated, 1, struct{}{}); err != nil {
		t.Fatal(err)
	}
}

func TestSetForTestRestoresThePreviousState(t *testing.T) {
	t.Run("inner", func(t *testing.T) {
		SetForTest(t, &Recorder{})
	})

	if current.Load() != nil {
		t.Error("broadcasting stayed routed to the test publisher")
	}
}
