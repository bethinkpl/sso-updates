package mq

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

// both services must key the events of a user the same way, the vectors are shared by sso and user-data
func TestOrderingKeys(t *testing.T) {
	data, err := os.ReadFile("testdata/ordering_keys.json")
	if err != nil {
		t.Fatal(err)
	}

	var vectors []struct {
		UserID      int64  `json:"user_id"`
		OrderingKey string `json:"ordering_key"`
	}

	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}

	if len(vectors) == 0 {
		t.Fatal("no ordering key vectors")
	}

	for _, vector := range vectors {
		if got := (Event{UserID: vector.UserID}).OrderingKey(); got != vector.OrderingKey {
			t.Errorf("user %d: ordering key %q, expected %q", vector.UserID, got, vector.OrderingKey)
		}
	}
}

func TestAttributes(t *testing.T) {
	before := time.Now().UTC().Truncate(time.Millisecond)

	event, err := NewEvent(EmailUpdated, 123456, map[string]int{"id": 123456})
	if err != nil {
		t.Fatal(err)
	}

	event.Source = testSource
	attributes := event.Attributes()

	var names []string
	for name := range attributes {
		names = append(names, name)
	}

	sort.Strings(names)

	expectedNames := []string{"event_id", "event_type", "occurred_at", "schema_version", "source", "user_id"}
	if len(names) != len(expectedNames) {
		t.Fatalf("attributes %v, expected %v", names, expectedNames)
	}

	for i := range names {
		if names[i] != expectedNames[i] {
			t.Fatalf("attributes %v, expected %v", names, expectedNames)
		}
	}

	if id, err := uuid.Parse(attributes["event_id"]); err != nil || id.Version() != 7 {
		t.Errorf("event_id %q is not a UUIDv7", attributes["event_id"])
	}

	expected := map[string]string{
		"event_type":     "email_updated",
		"source":         testSource,
		"schema_version": "1",
		"user_id":        "123456",
	}
	for name, value := range expected {
		if attributes[name] != value {
			t.Errorf("%s is %q, expected %q", name, attributes[name], value)
		}
	}

	occurredAt := attributes["occurred_at"]
	if !regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$`).MatchString(occurredAt) {
		t.Fatalf("occurred_at %q is not RFC 3339 UTC with milliseconds", occurredAt)
	}

	parsed, err := time.Parse(time.RFC3339, occurredAt)
	if err != nil || parsed.Before(before) || parsed.After(time.Now().UTC()) {
		t.Errorf("occurred_at %q is not the creation time", occurredAt)
	}
}

func TestOccurredAtIsSentInUTC(t *testing.T) {
	warsaw := time.FixedZone("CET", 60*60)
	event := Event{OccurredAt: time.Date(2026, 10, 29, 12, 0, 0, 123456789, warsaw)}

	if got := event.Attributes()["occurred_at"]; got != "2026-10-29T11:00:00.123Z" {
		t.Errorf("occurred_at is %q", got)
	}
}

func TestEventIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}

	for range 1000 {
		event, err := NewEvent(DeleteUser, 1, struct{}{})
		if err != nil {
			t.Fatal(err)
		}

		if seen[event.ID] {
			t.Fatalf("event ID %s generated twice", event.ID)
		}

		seen[event.ID] = true
	}
}

// the body is what json.Marshal has always produced, including its HTML escaping
func TestBodyIsTheMarshalledPayload(t *testing.T) {
	payload := struct {
		ID   int64  `json:"id"`
		Name string `json:"name,omitempty"`
	}{ID: 1, Name: "<Zażółć & gęślą>"}

	event, err := NewEvent(PersonalDataUpdated, 1, payload)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"id":1,"name":"\u003cZażółć \u0026 gęślą\u003e"}`
	if string(event.Body) != expected {
		t.Errorf("body %s, expected %s", event.Body, expected)
	}
}

func TestUnmarshallablePayload(t *testing.T) {
	if _, err := NewEvent(DeleteUser, 1, make(chan int)); err == nil {
		t.Error("a payload that cannot be marshalled was accepted")
	}
}
