// Package mq broadcasts user updates to the services that keep their own copy of the user data.
//
// Used by sso and user-data, the messages are described in README.md.
package mq

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// How long a broadcast may wait for the broker. It does not follow the request context: once the
// data has been changed, a client hanging up must not stop the broadcast.
const publishWaitTimeout = 11 * time.Second

// Sends an event to a broker
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type broker interface {
	Publisher
	Close(ctx context.Context) error
}

type state struct {
	source string
	broker broker
}

// nil until Init is called - broadcasting is then disabled, e.g. in tests
var current atomic.Pointer[state]

// Set up broadcasting. Call it once, before any request is served.
func Init(ctx context.Context, config Config) error {
	if config.Mode == "" {
		config.Mode = ModeRabbit
	}

	if err := config.validate(); err != nil {
		return err
	}

	log := config.Log
	if log == nil {
		log = logrus.StandardLogger()
	}

	broker, err := newBroker(ctx, config, log)
	if err != nil {
		return err
	}

	current.Store(&state{source: config.Source, broker: broker})

	return nil
}

// The broker(s) of the configured mode
func newBroker(ctx context.Context, config Config, log logrus.FieldLogger) (broker, error) {
	var rabbit, pubsub broker

	if config.usesRabbit() {
		rabbit = countedBroker{name: brokerRabbitMQ, broker: &rabbitBroker{dsn: config.RabbitMQDSN, log: log}}
	}

	if config.usesPubSub() {
		publisher, err := newPubSubBroker(ctx, config, log)
		if err != nil {
			return nil, err
		}

		pubsub = countedBroker{name: brokerPubSub, broker: publisher}
	}

	switch config.Mode {
	case ModeDual:
		// Pub/Sub first: once a consumer reads Pub/Sub, an event missing there is lost for it
		return dualBroker{first: pubsub, second: rabbit}, nil
	case ModePubSub:
		return pubsub, nil
	case ModeRabbit:
		return rabbit, nil
	}

	return nil, fmt.Errorf("unknown %s %q", envBrokerMode, config.Mode)
}

// Broadcast the payload as an event about the given user and wait until the broker confirms it.
func Broadcast(ctx context.Context, eventType EventType, userID int64, payload any) error {
	st := current.Load()
	if st == nil {
		logrus.WithField(fieldEventType, eventType).Warning("Broadcasting disabled as mq.Init has not been called")

		return nil
	}

	event, err := NewEvent(eventType, userID, payload)
	if err != nil {
		return err
	}

	event.Source = st.source

	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishWaitTimeout)
	defer cancel()

	if err := st.broker.Publish(waitCtx, event); err != nil {
		return fmt.Errorf("failed to broadcast the %s event %s: %w", event.Type, event.ID, err)
	}

	return nil
}

// Disconnect from the broker. Broadcasting is disabled afterwards.
func Close(ctx context.Context) error {
	st := current.Swap(nil)
	if st == nil {
		return nil
	}

	if err := st.broker.Close(ctx); err != nil {
		return fmt.Errorf("failed to close the broker: %w", err)
	}

	return nil
}

// Send the broadcasts to the given publisher until the test ends
func SetForTest(t interface{ Cleanup(f func()) }, publisher Publisher) {
	previous := current.Swap(&state{source: "test", broker: testBroker{publisher}})

	t.Cleanup(func() {
		current.Store(previous)
	})
}

type testBroker struct {
	Publisher
}

func (testBroker) Close(context.Context) error {
	return nil
}
