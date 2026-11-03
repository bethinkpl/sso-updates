package mq

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const testEventID = "event-1"

// Remembers the order of the calls across brokers
type stubBroker struct {
	name   string
	calls  *[]string
	events []Event
	err    error
	closed bool
}

func (s *stubBroker) Publish(_ context.Context, event Event) error {
	*s.calls = append(*s.calls, s.name)
	s.events = append(s.events, event)

	return s.err
}

func (s *stubBroker) Close(context.Context) error {
	s.closed = true

	return nil
}

func newDual(firstErr, secondErr error) (dualBroker, *stubBroker, *stubBroker, *[]string) {
	calls := &[]string{}
	first := &stubBroker{name: "first", calls: calls, err: firstErr}
	second := &stubBroker{name: "second", calls: calls, err: secondErr}

	return dualBroker{first: first, second: second}, first, second, calls
}

func TestDualPublishesTheSameEventToBothBrokersInOrder(t *testing.T) {
	dual, first, second, calls := newDual(nil, nil)

	if err := dual.Publish(context.Background(), Event{ID: testEventID}); err != nil {
		t.Fatal(err)
	}

	if strings.Join(*calls, ",") != "first,second" {
		t.Errorf("brokers called as %v", *calls)
	}

	if first.events[0].ID != testEventID || second.events[0].ID != testEventID {
		t.Error("the brokers got different events")
	}
}

func TestDualStopsWhenTheFirstBrokerFails(t *testing.T) {
	brokerDown := errors.New("broker down")
	dual, _, _, calls := newDual(brokerDown, nil)

	err := dual.Publish(context.Background(), Event{})
	if !errors.Is(err, brokerDown) || !strings.Contains(err.Error(), "not sent to any broker") {
		t.Errorf("unexpected error %v", err)
	}

	if strings.Join(*calls, ",") != "first" {
		t.Errorf("brokers called as %v, the second one must not be called", *calls)
	}
}

func TestDualReportsAFailureOfTheSecondBroker(t *testing.T) {
	brokerDown := errors.New("broker down")
	dual, _, second, _ := newDual(nil, brokerDown)

	err := dual.Publish(context.Background(), Event{})
	if !errors.Is(err, brokerDown) || !strings.Contains(err.Error(), "sent to the first broker only") {
		t.Errorf("unexpected error %v", err)
	}

	if len(second.events) != 1 {
		t.Error("the second broker was not called")
	}
}

func TestDualClosesBothBrokers(t *testing.T) {
	dual, first, second, _ := newDual(nil, nil)

	if err := dual.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !first.closed || !second.closed {
		t.Error("not every broker was closed")
	}
}
