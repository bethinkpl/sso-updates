package mq

import (
	"context"
	"sync"
)

// A publisher for tests (see SetForTest) that remembers every event it was given and fails
// the chosen event types.
type Recorder struct {
	mu       sync.Mutex
	events   []Event
	failures map[EventType]error
}

// Make publishing of the given event type fail with err
func (r *Recorder) FailOn(eventType EventType, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failures == nil {
		r.failures = map[EventType]error{}
	}

	r.failures[eventType] = err
}

func (r *Recorder) Publish(_ context.Context, event Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)

	return r.failures[event.Type]
}

// Every event given to Publish so far, including the failed ones, in order
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Event(nil), r.events...)
}
