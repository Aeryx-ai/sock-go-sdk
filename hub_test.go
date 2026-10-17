package sock

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
	"github.com/guygrigsby/sock-contract/gen/go/sock/v1/sockv1connect"
)

// hub is a messaging service with a pending queue that Ack drains.
type hub struct {
	sockv1connect.UnimplementedMessagingServiceHandler

	mu       sync.Mutex
	pending  []*sockv1.Message
	acks     [][]string
	keys     []string // idempotency keys, one per Send or NotifyOwner attempt
	failSend int      // Unavailable answers before a send succeeds
	// keepAcked leaves acknowledged messages pending: a hub whose acks do
	// not drain the queue.
	keepAcked bool
	// subscribe scripts each Subscribe attempt, numbered from 0.
	subscribe func(attempt int, ctx context.Context, s *connect.ServerStream[sockv1.SubscribeResponse]) error
	attempts  int
}

func (h *hub) FetchPending(_ context.Context, r *connect.Request[sockv1.FetchPendingRequest]) (*connect.Response[sockv1.FetchPendingResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := min(int(r.Msg.GetLimit()), len(h.pending))
	return connect.NewResponse(&sockv1.FetchPendingResponse{Messages: slices.Clone(h.pending[:n])}), nil
}

func (h *hub) Ack(_ context.Context, r *connect.Request[sockv1.AckRequest]) (*connect.Response[sockv1.AckResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := r.Msg.GetMessageIds()
	h.acks = append(h.acks, ids)
	if !h.keepAcked {
		h.pending = slices.DeleteFunc(h.pending, func(m *sockv1.Message) bool { return slices.Contains(ids, m.GetId()) })
	}
	return connect.NewResponse(&sockv1.AckResponse{}), nil
}

func (h *hub) sent(key, conversation, text string) (*sockv1.Message, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.keys = append(h.keys, key)
	if h.failSend > 0 {
		h.failSend--
		return nil, Unavailable()
	}
	return &sockv1.Message{Id: "m-" + key, ConversationId: conversation, Body: &sockv1.Message_Text{Text: text}}, nil
}

func (h *hub) Send(_ context.Context, r *connect.Request[sockv1.SendRequest]) (*connect.Response[sockv1.SendResponse], error) {
	m, err := h.sent(r.Msg.GetIdempotencyKey(), r.Msg.GetConversationId(), r.Msg.GetText())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sockv1.SendResponse{Message: m}), nil
}

func (h *hub) NotifyOwner(_ context.Context, r *connect.Request[sockv1.NotifyOwnerRequest]) (*connect.Response[sockv1.NotifyOwnerResponse], error) {
	m, err := h.sent(r.Msg.GetIdempotencyKey(), "owner", r.Msg.GetText())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sockv1.NotifyOwnerResponse{Message: m}), nil
}

func (h *hub) Subscribe(ctx context.Context, _ *connect.Request[sockv1.SubscribeRequest], s *connect.ServerStream[sockv1.SubscribeResponse]) error {
	h.mu.Lock()
	n := h.attempts
	h.attempts++
	h.mu.Unlock()
	return h.subscribe(n, ctx, s)
}

// serveHub starts h and returns a client for it whose retries do not sleep.
func serveHub(t *testing.T, h *hub) *Client {
	t.Helper()
	mux := http.NewServeMux()
	p, hh := sockv1connect.NewMessagingServiceHandler(h)
	mux.Handle(p, hh)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := New(Config{URL: srv.URL, Token: token}, "sock-test/1")
	c.Retrier.Sleep = func(context.Context, time.Duration) error { return nil }
	return c
}

func msgs(ids ...string) []*sockv1.Message {
	out := make([]*sockv1.Message, 0, len(ids))
	for _, id := range ids {
		out = append(out, &sockv1.Message{Id: id})
	}
	return out
}

func ids(ms []*sockv1.Message) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.GetId())
	}
	return out
}

var errHandler = errors.New("handler failed")
