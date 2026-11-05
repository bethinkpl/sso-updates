package mq

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// The environment variables of the configuration
const (
	envPubSubProjectID    = "PUBSUB_PROJECT_ID"
	envPubSubTopicID      = "PUBSUB_TOPIC_ID"
	envPubSubEndpoint     = "PUBSUB_ENDPOINT"
	envPubSubEmulatorHost = "PUBSUB_EMULATOR_HOST"
)

// The Pub/Sub endpoint in the region the services run in. Messages with an ordering key are delivered
// in order only when they are published in the same region.
const defaultPubSubEndpoint = "europe-west1-pubsub.googleapis.com:443"

type Config struct {
	// the service sending the events: "sso" or "user-data"
	Source          string
	PubSubProjectID string
	PubSubTopicID   string
	// empty when the client talks to the emulator (PUBSUB_EMULATOR_HOST)
	PubSubEndpoint string
	Log            logrus.FieldLogger
}

// Read the configuration from the environment variables
func ConfigFromEnv(source string, log logrus.FieldLogger) Config {
	endpoint := os.Getenv(envPubSubEndpoint)
	if endpoint == "" {
		endpoint = defaultPubSubEndpoint
	}

	// the Pub/Sub client connects to the emulator on its own
	if os.Getenv(envPubSubEmulatorHost) != "" {
		endpoint = ""
	}

	return Config{
		Source:          source,
		PubSubProjectID: os.Getenv(envPubSubProjectID),
		PubSubTopicID:   os.Getenv(envPubSubTopicID),
		PubSubEndpoint:  endpoint,
		Log:             log,
	}
}

// Report the settings that are missing
func (c Config) validate() error {
	var missing []string

	if c.PubSubProjectID == "" {
		missing = append(missing, envPubSubProjectID)
	}

	if c.PubSubTopicID == "" {
		missing = append(missing, envPubSubTopicID)
	}

	if len(missing) > 0 {
		return fmt.Errorf("broadcasting needs the %s env var(s)", strings.Join(missing, ", "))
	}

	return nil
}
