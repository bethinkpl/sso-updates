package mq

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// Where the broadcasts are sent
type Mode string

const (
	// RabbitMQ only, the default
	ModeRabbit Mode = "rabbit"
	// both RabbitMQ and Pub/Sub, while the consumers move to Pub/Sub
	ModeDual Mode = "dual"
	// Pub/Sub only
	ModePubSub Mode = "pubsub"
)

// The environment variables of the configuration
const (
	envBrokerMode         = "BROKER_MODE"
	envRabbitMQDSN        = "RABBITMQ_DSN"
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
	Source string
	// empty means ModeRabbit
	Mode            Mode
	RabbitMQDSN     string
	PubSubProjectID string
	PubSubTopicID   string
	// empty when the client talks to the emulator (PUBSUB_EMULATOR_HOST)
	PubSubEndpoint string
	Log            logrus.FieldLogger
}

// Read the configuration from the environment variables
func ConfigFromEnv(source string, log logrus.FieldLogger) Config {
	mode := Mode(os.Getenv(envBrokerMode))
	if mode == "" {
		mode = ModeRabbit
	}

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
		Mode:            mode,
		RabbitMQDSN:     os.Getenv(envRabbitMQDSN),
		PubSubProjectID: os.Getenv(envPubSubProjectID),
		PubSubTopicID:   os.Getenv(envPubSubTopicID),
		PubSubEndpoint:  endpoint,
		Log:             log,
	}
}

func (c Config) usesRabbit() bool {
	return c.Mode == ModeRabbit || c.Mode == ModeDual
}

func (c Config) usesPubSub() bool {
	return c.Mode == ModePubSub || c.Mode == ModeDual
}

// Report a mode that does not exist or the settings missing for it
func (c Config) validate() error {
	switch c.Mode {
	case ModeRabbit, ModeDual, ModePubSub:
	default:
		return fmt.Errorf("%s %q is not one of: %s, %s, %s", envBrokerMode, c.Mode, ModeRabbit, ModeDual, ModePubSub)
	}

	var missing []string

	if c.usesRabbit() && c.RabbitMQDSN == "" {
		missing = append(missing, envRabbitMQDSN)
	}

	if c.usesPubSub() && c.PubSubProjectID == "" {
		missing = append(missing, envPubSubProjectID)
	}

	if c.usesPubSub() && c.PubSubTopicID == "" {
		missing = append(missing, envPubSubTopicID)
	}

	if len(missing) > 0 {
		return fmt.Errorf("%s %s needs the %s env var(s)", envBrokerMode, c.Mode, strings.Join(missing, ", "))
	}

	return nil
}
