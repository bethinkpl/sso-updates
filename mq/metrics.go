package mq

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// The broker name in the metrics and the errors
const brokerPubSub = "pubsub"

// Every broadcast per broker
var broadcastsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "mq_broadcasts_total",
		Help: "Number of broadcasts, by broker, event type and result.",
	},
	[]string{"broker", fieldEventType, "result"},
)

// Expose the broadcast metrics. Call it once, next to the other metrics.
func RegisterMetrics(registerer prometheus.Registerer) error {
	if err := registerer.Register(broadcastsTotal); err != nil {
		return fmt.Errorf("failed to register the broadcast metrics: %w", err)
	}

	return nil
}

// Counts the broadcasts of one broker and names it in the errors
type countedBroker struct {
	name string
	broker
}

func (b countedBroker) Publish(ctx context.Context, event Event) error {
	err := b.broker.Publish(ctx, event)

	result := "success"
	if err != nil {
		result = "error"
	}

	broadcastsTotal.WithLabelValues(b.name, string(event.Type), result).Inc()

	if err != nil {
		return fmt.Errorf("%s: %w", b.name, err)
	}

	return nil
}
