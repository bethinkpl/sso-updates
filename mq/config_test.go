package mq

import (
	"context"
	"strings"
	"testing"
)

const testRabbitMQDSN = "amqp://rabbit:5672/"

var brokerEnvVars = []string{
	envBrokerMode, envRabbitMQDSN, envPubSubProjectID, envPubSubTopicID, envPubSubEndpoint, envPubSubEmulatorHost,
}

// start every test from an empty broker configuration
func clearBrokerEnv(t *testing.T) {
	t.Helper()

	for _, name := range brokerEnvVars {
		t.Setenv(name, "")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	clearBrokerEnv(t)

	config := ConfigFromEnv("user-data", nil)
	if config.Source != "user-data" || config.Mode != ModeRabbit || config.PubSubEndpoint != defaultPubSubEndpoint {
		t.Errorf("unexpected configuration %+v", config)
	}
}

func TestConfigFromEnv(t *testing.T) {
	clearBrokerEnv(t)
	t.Setenv(envBrokerMode, string(ModeDual))
	t.Setenv(envRabbitMQDSN, testRabbitMQDSN)
	t.Setenv(envPubSubProjectID, "bethink-development")
	t.Setenv(envPubSubTopicID, testTopic)
	t.Setenv(envPubSubEndpoint, "europe-central2-pubsub.googleapis.com:443")

	config := ConfigFromEnv(testSource, nil)
	expected := Config{
		Source:          testSource,
		Mode:            ModeDual,
		RabbitMQDSN:     testRabbitMQDSN,
		PubSubProjectID: "bethink-development",
		PubSubTopicID:   testTopic,
		PubSubEndpoint:  "europe-central2-pubsub.googleapis.com:443",
	}

	if config != expected {
		t.Errorf("configuration %+v, expected %+v", config, expected)
	}
}

// the client connects to the emulator on its own and must not be given an endpoint
func TestConfigFromEnvWithAnEmulator(t *testing.T) {
	clearBrokerEnv(t)
	t.Setenv(envPubSubEmulatorHost, "localhost:8085")
	t.Setenv(envPubSubEndpoint, "europe-central2-pubsub.googleapis.com:443")

	if config := ConfigFromEnv(testSource, nil); config.PubSubEndpoint != "" {
		t.Errorf("endpoint %q set while using the emulator", config.PubSubEndpoint)
	}
}

func TestValidate(t *testing.T) {
	rabbit := testRabbitMQDSN
	pubsubOnly := Config{Mode: ModePubSub, PubSubProjectID: testProject, PubSubTopicID: testTopic}

	cases := []struct {
		name    string
		config  Config
		missing []string
	}{
		{"rabbit", Config{Mode: ModeRabbit, RabbitMQDSN: rabbit}, nil},
		{"rabbit without DSN", Config{Mode: ModeRabbit}, []string{envRabbitMQDSN}},
		{"pubsub mode", pubsubOnly, nil},
		{"pubsub without project", Config{Mode: ModePubSub, PubSubTopicID: testTopic}, []string{envPubSubProjectID}},
		{"pubsub without topic", Config{Mode: ModePubSub, PubSubProjectID: testProject}, []string{envPubSubTopicID}},
		{
			"dual",
			Config{Mode: ModeDual, RabbitMQDSN: rabbit, PubSubProjectID: testProject, PubSubTopicID: testTopic},
			nil,
		},
		{"dual without anything", Config{Mode: ModeDual}, []string{envRabbitMQDSN, envPubSubProjectID, envPubSubTopicID}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.config.validate()

			if len(c.missing) == 0 && err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if len(c.missing) > 0 && err == nil {
				t.Fatalf("no error, expected %v to be missing", c.missing)
			}

			for _, name := range c.missing {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error %q does not name %s", err, name)
				}
			}
		})
	}
}

func TestValidateRejectsAnUnknownMode(t *testing.T) {
	err := Config{Mode: "kafka", RabbitMQDSN: testRabbitMQDSN}.validate()
	if err == nil || !strings.Contains(err.Error(), "kafka") {
		t.Errorf("unexpected error %v", err)
	}
}

func TestInitRejectsAnIncompleteConfiguration(t *testing.T) {
	if err := Init(context.Background(), Config{Mode: ModeDual, RabbitMQDSN: testRabbitMQDSN}); err == nil {
		t.Fatal("Init accepted dual mode without the Pub/Sub settings")
	}

	if current.Load() != nil {
		t.Error("a failed Init enabled broadcasting")
	}
}
