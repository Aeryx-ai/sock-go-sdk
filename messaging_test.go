package sock

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

// One key per call, reused by every retry: the hub dedupes the retry.
func TestSendReusesOneKeyAcrossRetries(t *testing.T) {
	h := &hub{failSend: 2}
	c := serveHub(t, h)
	m, err := c.Send(t.Context(), "conv-1", "hi", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(h.keys) != 3 || h.keys[0] == "" || h.keys[1] != h.keys[0] || h.keys[2] != h.keys[0] {
		t.Fatalf("keys %q", h.keys)
	}
	if m.GetConversationId() != "conv-1" || m.GetText() != "hi" {
		t.Fatalf("message %v", m)
	}
	if _, err := c.Send(t.Context(), "conv-1", "again", ""); err != nil {
		t.Fatal(err)
	}
	if h.keys[3] == h.keys[0] {
		t.Fatal("a second call reused the first call's key")
	}
}

func TestSendAndNotifyOwnerUseTheCallersKey(t *testing.T) {
	h := &hub{}
	c := serveHub(t, h)
	if _, err := c.Send(t.Context(), "conv-1", "hi", "run-42"); err != nil {
		t.Fatal(err)
	}
	m, err := c.NotifyOwner(t.Context(), "done", "run-43")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h.keys, []string{"run-42", "run-43"}) || m.GetConversationId() != "owner" {
		t.Fatalf("keys %q message %v", h.keys, m)
	}
}

func TestInvalidKeyIsRefusedBeforeAnyCall(t *testing.T) {
	h := &hub{}
	c := serveHub(t, h)
	for _, k := range []string{"-lead", "has space", strings.Repeat("a", 129), "semi;colon"} {
		if _, err := c.Send(t.Context(), "conv-1", "hi", k); !errors.Is(err, ErrInvalidIdempotencyKey) {
			t.Errorf("Send key %q: %v", k, err)
		}
		if _, err := c.NotifyOwner(t.Context(), "hi", k); !errors.Is(err, ErrInvalidIdempotencyKey) {
			t.Errorf("NotifyOwner key %q: %v", k, err)
		}
	}
	if len(h.keys) != 0 {
		t.Fatalf("hub saw %q", h.keys)
	}
}

func TestValidIdempotencyKey(t *testing.T) {
	for k, want := range map[string]bool{
		"a": true, "A1._:-z": true, strings.Repeat("a", 128): true,
		"": false, ".a": false, "_a": false, strings.Repeat("a", 129): false, "a/b": false, "é": false,
	} {
		if got := ValidIdempotencyKey(k); got != want {
			t.Errorf("ValidIdempotencyKey(%q) = %v, want %v", k, got, want)
		}
	}
}

func TestAckChunksAtTheLimit(t *testing.T) {
	h := &hub{}
	c := serveHub(t, h)
	all := make([]string, 1200)
	for i := range all {
		all[i] = fmt.Sprint(i)
	}
	if err := c.Ack(t.Context(), all...); err != nil {
		t.Fatal(err)
	}
	if len(h.acks) != 3 || len(h.acks[0]) != AckMax || len(h.acks[1]) != AckMax || len(h.acks[2]) != 200 {
		t.Fatalf("ack sizes %d", len(h.acks))
	}
	if err := c.Ack(t.Context()); err != nil || len(h.acks) != 3 {
		t.Fatalf("empty ack: %v, %d requests", err, len(h.acks))
	}
}

func TestDrainHandlesEveryBatchInOrderAndAcks(t *testing.T) {
	var queue []string
	for i := range 250 {
		queue = append(queue, fmt.Sprint(i))
	}
	h := &hub{pending: msgs(queue...)}
	c := serveHub(t, h)
	var got []string
	err := c.Drain(t.Context(), func(_ context.Context, m *sockv1.Message) error {
		got = append(got, m.GetId())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, queue) || len(h.pending) != 0 || len(h.acks) != 3 {
		t.Fatalf("handled %d, pending %d, acks %d", len(got), len(h.pending), len(h.acks))
	}
}

// what the handler finished is acknowledged; the failed message and the
// rest stay pending
func TestDrainAcksWhatWasHandledWhenTheHandlerFails(t *testing.T) {
	h := &hub{pending: msgs("a", "b", "c")}
	c := serveHub(t, h)
	err := c.Drain(t.Context(), func(_ context.Context, m *sockv1.Message) error {
		if m.GetId() == "b" {
			return errHandler
		}
		return nil
	})
	if !errors.Is(err, errHandler) {
		t.Fatalf("err %v", err)
	}
	if got := ids(h.pending); !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("pending %q", got)
	}
}

func TestDrainStopsWhenAcksDoNotDrain(t *testing.T) {
	h := &hub{pending: msgs("a"), keepAcked: true}
	c := serveHub(t, h)
	calls := 0
	err := c.Drain(t.Context(), func(context.Context, *sockv1.Message) error { calls++; return nil })
	if !errors.Is(err, ErrNotDraining) || calls != 1 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
}

func TestEndsSessionAndAlreadyDone(t *testing.T) {
	reason := func(r sockv1.ErrorReason) error {
		return withReason(connect.NewError(connect.CodePermissionDenied, nil), r)
	}
	if !EndsSession(reason(sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID)) || !EndsSession(Describe(reason(sockv1.ErrorReason_ERROR_REASON_ACCOUNT_SUSPENDED))) {
		t.Fatal("a rejected token or suspended account ends the session")
	}
	if EndsSession(NotMember()) || EndsSession(Unavailable()) || EndsSession(nil) {
		t.Fatal("only token and suspension refusals end the session")
	}
	if !AlreadyDone(NotMember()) || !AlreadyDone(reason(sockv1.ErrorReason_ERROR_REASON_NOT_FOUND)) || AlreadyDone(Unavailable()) {
		t.Fatal("AlreadyDone misclassifies")
	}
}
