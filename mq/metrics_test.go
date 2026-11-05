package mq

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type stubBroker struct {
	err error
}

func (s *stubBroker) Publish(context.Context, Event) error {
	return s.err
}

func (s *stubBroker) Close(context.Context) error {
	return nil
}

func countOf(eventType EventType, result string) float64 {
	return testutil.ToFloat64(broadcastsTotal.WithLabelValues(brokerPubSub, string(eventType), result))
}

func TestCountedBrokerCountsTheBroadcasts(t *testing.T) {
	stub := &stubBroker{}
	counted := countedBroker{name: brokerPubSub, broker: stub}
	before := countOf(ForceLogout, "success")

	if err := counted.Publish(context.Background(), Event{Type: ForceLogout}); err != nil {
		t.Fatal(err)
	}

	if after := countOf(ForceLogout, "success"); after != before+1 {
		t.Errorf("success count went from %v to %v", before, after)
	}
}

func TestCountedBrokerCountsAndNamesTheFailures(t *testing.T) {
	brokerDown := errors.New("broker down")
	stub := &stubBroker{err: brokerDown}
	counted := countedBroker{name: brokerPubSub, broker: stub}
	before := countOf(DeleteUser, "error")

	err := counted.Publish(context.Background(), Event{Type: DeleteUser})
	if !errors.Is(err, brokerDown) || !strings.HasPrefix(err.Error(), brokerPubSub+": ") {
		t.Errorf("unexpected error %v", err)
	}

	if after := countOf(DeleteUser, "error"); after != before+1 {
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
