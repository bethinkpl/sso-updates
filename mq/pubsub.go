package mq

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// How long the client may try to publish a message, retries included. It stays below
// publishWaitTimeout, so that waiting for the result never gives up before the client does.
const pubsubPublishTimeout = 10 * time.Second

// Publishes to a Pub/Sub topic with the user ID as the ordering key, so that the events of a user
// are delivered in the order they were published.
type pubsubBroker struct {
	client    *pubsub.Client
	publisher *pubsub.Publisher
	log       logrus.FieldLogger
}

// One client and one publisher for the whole process - both are safe for concurrent use.
// No connection is checked here: the publisher role is not allowed to look the topic up.
func newPubSubBroker(
	ctx context.Context,
	config Config,
	log logrus.FieldLogger,
	opts ...option.ClientOption,
) (*pubsubBroker, error) {
	if config.PubSubEndpoint != "" {
		opts = append(opts, option.WithEndpoint(config.PubSubEndpoint))
	}

	client, err := pubsub.NewClient(ctx, config.PubSubProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create the Pub/Sub client: %w", err)
	}

	publisher := client.Publisher(config.PubSubTopicID)
	publisher.EnableMessageOrdering = true
	publisher.PublishSettings.Timeout = pubsubPublishTimeout

	return &pubsubBroker{client: client, publisher: publisher, log: log}, nil
}

// Publish the event and wait until Pub/Sub has stored it
func (b *pubsubBroker) Publish(ctx context.Context, event Event) error {
	key := event.OrderingKey()
	log := b.log.WithFields(logrus.Fields{
		fieldEventID:   event.ID,
		fieldEventType: event.Type,
		fieldUserID:    event.UserID,
	})

	result := b.publisher.Publish(ctx, &pubsub.Message{
		Data:        event.Body,
		Attributes:  event.Attributes(),
		OrderingKey: key,
	})

	messageID, err := result.Get(ctx)
	if err != nil {
		// a failed publish pauses its ordering key: every later event of this user would fail too
		b.publisher.ResumePublish(key)
		log.WithError(err).Error("Failed to publish to Pub/Sub")

		return fmt.Errorf("failed to publish to Pub/Sub: %w", err)
	}

	log.WithField("message_id", messageID).Info("Published to Pub/Sub")

	return nil
}

// Send the messages still in flight and disconnect
func (b *pubsubBroker) Close(ctx context.Context) error {
	stopped := make(chan struct{})

	go func() {
		b.publisher.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-ctx.Done():
		return fmt.Errorf("the Pub/Sub publisher did not stop in time: %w", ctx.Err())
	}

	if err := b.client.Close(); err != nil {
		return fmt.Errorf("failed to close the Pub/Sub client: %w", err)
	}

	return nil
}
