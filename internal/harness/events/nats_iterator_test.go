package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// scriptedEventConsumer only implements Next; the other Consumer methods are
// embedded because the iterator must not invoke them.
type scriptedEventConsumer struct {
	jetstream.Consumer
	messages []jetstream.Msg
	calls    int
}

func (c *scriptedEventConsumer) Next(_ ...jetstream.FetchOpt) (jetstream.Msg, error) {
	c.calls++
	if len(c.messages) == 0 {
		return nil, errors.New("no more scripted messages")
	}
	msg := c.messages[0]
	c.messages = c.messages[1:]
	return msg, nil
}

type scriptedEventMsg struct {
	jetstream.Msg
	data []byte
}

func (m scriptedEventMsg) Data() []byte { return m.data }

func eventMessage(t *testing.T, id EventID, trace string) jetstream.Msg {
	t.Helper()
	data, err := json.Marshal(&Event{ID: id, TraceID: trace, Type: EventStepStart})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return scriptedEventMsg{data: data}
}

func TestNATSEventIteratorReturnsFirstEventWithoutBatchPrefetch(t *testing.T) {
	consumer := &scriptedEventConsumer{
		messages: []jetstream.Msg{eventMessage(t, "first", "trace")},
	}
	iter := &natsEventIterator{consumer: consumer}
	got, ok := iter.Next(t.Context())
	if !ok || got.ID != "first" {
		t.Fatalf("first event: got %+v, ok=%v", got, ok)
	}
	if consumer.calls != 1 {
		t.Fatalf("Next calls = %d, want 1 (no fixed-size prefetch)", consumer.calls)
	}
}

func TestNATSEventIteratorKeepsEventsBeforeReadError(t *testing.T) {
	consumer := &scriptedEventConsumer{messages: []jetstream.Msg{
		eventMessage(t, "first", "trace"),
		eventMessage(t, "second", "trace"),
	}}
	iter := &natsEventIterator{consumer: consumer}
	for _, id := range []EventID{"first", "second"} {
		got, ok := iter.Next(t.Context())
		if !ok || got.ID != id {
			t.Fatalf("event %s: got %+v, ok=%v", id, got, ok)
		}
	}
	if got, ok := iter.Next(t.Context()); ok || got != nil {
		t.Fatalf("exhausted iterator: got %+v, ok=%v", got, ok)
	}
	if consumer.calls != 3 {
		t.Fatalf("Next calls = %d, want 3", consumer.calls)
	}
}

func TestNATSEventIteratorSkipsMalformedAndFilteredMessages(t *testing.T) {
	consumer := &scriptedEventConsumer{messages: []jetstream.Msg{
		scriptedEventMsg{data: []byte("not JSON")},
		eventMessage(t, "wrong-trace", "other"),
		eventMessage(t, "matching", "wanted"),
	}}
	iter := &natsEventIterator{consumer: consumer, filter: EventFilter{TraceID: "wanted"}}
	got, ok := iter.Next(context.Background())
	if !ok || got.ID != "matching" {
		t.Fatalf("filtered event: got %+v, ok=%v", got, ok)
	}
	if consumer.calls != 3 {
		t.Fatalf("Next calls = %d, want 3", consumer.calls)
	}
}
