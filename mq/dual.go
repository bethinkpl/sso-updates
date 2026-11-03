package mq

import (
	"context"
	"errors"
	"fmt"
)

// Publishes every event to both brokers while the consumers move from one to the other.
//
// It stops at the first failure: the request then fails and is retried as a whole. Publishing to
// the second broker after the first one failed could leave an event the database never got
// (user-data broadcasts before writing) on the broker the consumers still read.
type dualBroker struct {
	first  broker
	second broker
}

func (b dualBroker) Publish(ctx context.Context, event Event) error {
	if err := b.first.Publish(ctx, event); err != nil {
		return fmt.Errorf("not sent to any broker: %w", err)
	}

	if err := b.second.Publish(ctx, event); err != nil {
		return fmt.Errorf("sent to the first broker only: %w", err)
	}

	return nil
}

func (b dualBroker) Close(ctx context.Context) error {
	return errors.Join(b.first.Close(ctx), b.second.Close(ctx))
}
