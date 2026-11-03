package mq

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func countOf(broker string, eventType EventType, result string) float64 {
	return testutil.ToFloat64(broadcastsTotal.WithLabelValues(broker, string(eventType), result))
}

func TestCountedBrokerCountsTheBroadcasts(t *testing.T) {
	stub := &stubBroker{name: brokerPubSub, calls: &[]string{}}
	counted := countedBroker{name: brokerPubSub, broker: stub}
	before := countOf(brokerPubSub, ForceLogout, "success")

	if err := counted.Publish(context.Background(), Event{Type: ForceLogout}); err != nil {
		t.Fatal(err)
	}

	if after := countOf(brokerPubSub, ForceLogout, "success"); after != before+1 {
		t.Errorf("success count went from %v to %v", before, after)
	}
}

func TestCountedBrokerCountsAndNamesTheFailures(t *testing.T) {
	brokerDown := errors.New("broker down")
	stub := &stubBroker{name: brokerRabbitMQ, calls: &[]string{}, err: brokerDown}
	counted := countedBroker{name: brokerRabbitMQ, broker: stub}
	before := countOf(brokerRabbitMQ, DeleteUser, "error")

	err := counted.Publish(context.Background(), Event{Type: DeleteUser})
	if !errors.Is(err, brokerDown) || !strings.HasPrefix(err.Error(), brokerRabbitMQ+": ") {
		t.Errorf("unexpected error %v", err)
	}

	if after := countOf(brokerRabbitMQ, DeleteUser, "error"); after != before+1 {
		t.Errorf("error count went from %v to %v", before, after)
	}
}

func TestRegisterMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()

	if err := RegisterMetrics(registry); err != nil {
		t.Fatal(err)
	}

	if err := RegisterMetrics(registry); err == nil {
		t.Error("the metrics were registered twice")
	}
}
