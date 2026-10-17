package sock

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
)

// waits records Listen's sleeps and ends it after stopAfter of them.
type waits struct {
	mu        sync.Mutex
	got       []time.Duration
	stopAfter int
	cancel    context.CancelFunc
}

func (w *waits) sleep(_ context.Context, d time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.got = append(w.got, d)
	if len(w.got) >= w.stopAfter {
		w.cancel()
		return context.Canceled
	}
	return nil
}

func listenOpts(w *waits) ListenOptions {
	return ListenOptions{Sleep: w.sleep, Jitter: func(d time.Duration) time.Duration { return d }}
}

func send(s *connect.ServerStream[sockv1.SubscribeResponse], ids ...string) error {
	for _, id := range ids {
		if err := s.Send(&sockv1.SubscribeResponse{Event: &sockv1.SubscribeResponse_Message{Message: &sockv1.Message{Id: id}}}); err != nil {
			return err
		}
	}
	return nil
}

func TestListenDeliversInOrderAndReconnects(t *testing.T) {
	h := &hub{subscribe: func(n int, _ context.Context, s *connect.ServerStream[sockv1.SubscribeResponse]) error {
		if n == 0 {
			return send(s, "a", "b")
		}
		return send(s, "c")
	}}
	c := serveHub(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	w := &waits{stopAfter: 3, cancel: cancel}
	var got []string
	err := c.Listen(ctx, func(_ context.Context, m *sockv1.Message) error {
		got = append(got, m.GetId())
		return nil
	}, listenOpts(w))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"a", "b", "c", "c"}) {
		t.Fatalf("got %q", got)
	}
	// every stream received something, so the backoff never grows
	if !slices.Equal(w.got, []time.Duration{time.Second, time.Second, time.Second}) {
		t.Fatalf("waits %v", w.got)
	}
}

// A transient handler failure ends the attempt and the message comes back
// first; the backoff keeps growing, since redelivering it is no sign the
// handler will succeed.
func TestListenReplaysAFailedMessageFirst(t *testing.T) {
	h := &hub{subscribe: func(_ int, _ context.Context, s *connect.ServerStream[sockv1.SubscribeResponse]) error {
		return send(s, "a", "b")
	}}
	c := serveHub(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	w := &waits{stopAfter: 3, cancel: cancel}
	var got []string
	_ = c.Listen(ctx, func(_ context.Context, m *sockv1.Message) error {
		got = append(got, m.GetId())
		return Unavailable()
	}, listenOpts(w))
	if !slices.Equal(got, []string{"a", "a", "a"}) {
		t.Fatalf("got %q", got)
	}
	if !slices.Equal(w.got, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}) {
		t.Fatalf("waits %v", w.got)
	}
}

// A handler error is the handler's, never a stream refusal: one that would
// fail again on redelivery (a poison message, a call the token may not make)
// ends Listen with the message pending instead of redelivering it forever or
// reading as the hub refusing the subscription.
func TestListenReturnsAHandlersPermanentError(t *testing.T) {
	for name, herr := range map[string]error{
		"plain":              errHandler,
		"invalid argument":   connect.NewError(connect.CodeInvalidArgument, errors.New("bad text")),
		"caller not allowed": withReason(connect.NewError(connect.CodePermissionDenied, nil), sockv1.ErrorReason_ERROR_REASON_CALLER_NOT_ALLOWED),
	} {
		t.Run(name, func(t *testing.T) {
			h := &hub{subscribe: func(_ int, _ context.Context, s *connect.ServerStream[sockv1.SubscribeResponse]) error {
				return send(s, "a", "b")
			}}
			c := serveHub(t, h)
			ctx, cancel := context.WithCancel(t.Context())
			w := &waits{stopAfter: 2, cancel: cancel}
			var got []string
			err := c.Listen(ctx, func(_ context.Context, m *sockv1.Message) error {
				got = append(got, m.GetId())
				return herr
			}, listenOpts(w))
			he, ok := errors.AsType[*HandlerError](err)
			if !ok || he.MessageID != "a" || !errors.Is(err, herr) || len(w.got) != 0 || !slices.Equal(got, []string{"a"}) {
				t.Fatalf("err %v, waits %v, handled %q", err, w.got, got)
			}
		})
	}
}

func TestListenHonorsAHandlersWaitError(t *testing.T) {
	h := &hub{subscribe: func(_ int, _ context.Context, s *connect.ServerStream[sockv1.SubscribeResponse]) error {
		return send(s, "a")
	}}
	c := serveHub(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	w := &waits{stopAfter: 1, cancel: cancel}
	_ = c.Listen(ctx, func(context.Context, *sockv1.Message) error {
		return &WaitError{After: 3 * time.Hour, Err: errHandler}
	}, listenOpts(w))
	if !slices.Equal(w.got, []time.Duration{3 * time.Hour}) {
		t.Fatalf("waits %v", w.got)
	}
}

func TestListenStopsWhenTheHubRefusesTheCallerForGood(t *testing.T) {
	for _, r := range []sockv1.ErrorReason{
		sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID,
		sockv1.ErrorReason_ERROR_REASON_ACCOUNT_SUSPENDED,
		sockv1.ErrorReason_ERROR_REASON_CALLER_NOT_ALLOWED,
	} {
		t.Run(r.String(), func(t *testing.T) {
			h := &hub{subscribe: func(int, context.Context, *connect.ServerStream[sockv1.SubscribeResponse]) error {
				return withReason(connect.NewError(connect.CodeUnauthenticated, nil), r)
			}}
			c := serveHub(t, h)
			w := &waits{stopAfter: 1, cancel: func() {}}
			err := c.Listen(t.Context(), func(context.Context, *sockv1.Message) error { return nil }, listenOpts(w))
			if Reason(err) != r || len(w.got) != 0 {
				t.Fatalf("err %v (reason %v), waits %v", err, Reason(err), w.got)
			}
		})
	}
}

// a refusal that may heal (lapsed Sock Pro) is retried, slowly
func TestListenRetriesARefusalAMinuteApart(t *testing.T) {
	h := &hub{subscribe: func(int, context.Context, *connect.ServerStream[sockv1.SubscribeResponse]) error {
		return withReason(connect.NewError(connect.CodePermissionDenied, nil), sockv1.ErrorReason_ERROR_REASON_SUBSCRIPTION_REQUIRED)
	}}
	c := serveHub(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	w := &waits{stopAfter: 2, cancel: cancel}
	if err := c.Listen(ctx, nil, listenOpts(w)); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(w.got, []time.Duration{time.Minute, time.Minute}) {
		t.Fatalf("waits %v", w.got)
	}
}

func TestListenWaitsForRetryAfterCappedAtAnHour(t *testing.T) {
	for hdr, want := range map[string]time.Duration{"30": 30 * time.Second, "86400": time.Hour} {
		t.Run(hdr, func(t *testing.T) {
			h := &hub{subscribe: func(int, context.Context, *connect.ServerStream[sockv1.SubscribeResponse]) error {
				return RateLimited(hdr)
			}}
			c := serveHub(t, h)
			ctx, cancel := context.WithCancel(t.Context())
			w := &waits{stopAfter: 1, cancel: cancel}
			_ = c.Listen(ctx, nil, listenOpts(w))
			if !slices.Equal(w.got, []time.Duration{want}) {
				t.Fatalf("waits %v", w.got)
			}
		})
	}
}

// a half-open stream sends nothing: the watchdog ends it and Listen reconnects
func TestListenReconnectsASilentStream(t *testing.T) {
	h := &hub{subscribe: func(_ int, ctx context.Context, _ *connect.ServerStream[sockv1.SubscribeResponse]) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	c := serveHub(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	w := &waits{stopAfter: 1, cancel: cancel}
	var reconnectErr error
	o := listenOpts(w)
	o.Idle = 50 * time.Millisecond
	o.OnReconnect = func(err error, _ time.Duration) { reconnectErr = err }
	if err := c.Listen(ctx, nil, o); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(reconnectErr, ErrStreamIdle) {
		t.Fatalf("reconnect after %v", reconnectErr)
	}
}

func TestEqualJitterStaysInTheUpperHalf(t *testing.T) {
	for range 1000 {
		if d := equalJitter(4 * time.Second); d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("jitter %v", d)
		}
	}
	if equalJitter(1) != 1 || equalJitter(0) != 0 {
		t.Fatal("tiny durations pass through")
	}
}
