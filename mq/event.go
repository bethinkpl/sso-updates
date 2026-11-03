package mq

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// What happened to a user. Consumers get it as the "event_type" attribute.
type EventType string

// The JSON body of each type is described in README.md - its shape must not change.
const (
	EmailUpdated        EventType = "email_updated"
	ForceLogout         EventType = "force_logout"
	PersonalDataUpdated EventType = "personal_data_updated"
	DeleteUser          EventType = "delete_user"
)

// Version of the attributes set below. Change it only together with the consumers.
const SchemaVersion = "1"

// Names shared by the attributes, the log fields and the metric labels
const (
	fieldEventID   = "event_id"
	fieldEventType = "event_type"
	fieldUserID    = "user_id"
)

// RFC 3339 in UTC with milliseconds, e.g. 2026-10-29T10:00:00.123Z
const occurredAtFormat = "2006-01-02T15:04:05.000Z"

// A single broadcast: the JSON body the consumers parse and the metadata sent next to it
// (Pub/Sub attributes, AMQP properties).
type Event struct {
	// unique per event, lets consumers drop the duplicates
	ID     string
	Type   EventType
	Source string
	UserID int64
	// when the event was created, i.e. right after (sso) or before (user-data) the database write
	OccurredAt time.Time
	Body       []byte
}

// Build an event with a fresh ID. The body is json.Marshal(payload) - byte for byte what has
// always been broadcast.
func NewEvent(eventType EventType, userID int64, payload any) (Event, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("failed to marshal the payload: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("failed to generate the event ID: %w", err)
	}

	return Event{
		ID:         id.String(),
		Type:       eventType,
		UserID:     userID,
		OccurredAt: time.Now().UTC(),
		Body:       body,
	}, nil
}

// Keeps the events of one user in order: the user ID in decimal, e.g. "123456".
// sso and user-data must produce the same key for the same user.
func (e Event) OrderingKey() string {
	return strconv.FormatInt(e.UserID, 10)
}

// The metadata sent next to the body
func (e Event) Attributes() map[string]string {
	return map[string]string{
		fieldEventID:     e.ID,
		fieldEventType:   string(e.Type),
		"source":         e.Source,
		"occurred_at":    e.OccurredAt.UTC().Format(occurredAtFormat),
		"schema_version": SchemaVersion,
		fieldUserID:      e.OrderingKey(),
	}
}
