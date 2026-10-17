package sock

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

const (
	// heartbeatInterval is how often the hub sends a heartbeat on an idle
	// stream (sock.v1 Subscribe contract).
	heartbeatInterval = 30 * time.Second
	// DefaultIdle tolerates one missed heartbeat.
	DefaultIdle   = 2 * heartbeatInterval
	reconnectBase = time.Second
	// reconnectMax tops out the backoff between stream attempts. It is short
	// on purpose: a hub deploy or restart must not leave a listener dark for
	// a minute, and one reconnect attempt every few seconds is cheap.
	reconnectMax = 5 * time.Second
	// refusalBackoff paces reconnects after a refusal that does not heal on
	// its own soon (lapsed Sock Pro, a wrong URL).
	refusalBackoff = time.Minute
	// maxRetryAfter bounds a hub Retry-After, honored in full below it:
	// reconnecting sooner is what the header says not to do.
	maxRetryAfter = time.Hour
)

// ErrStreamIdle marks a stream that went silent past the idle limit,
// typically a half-open connection. Listen reconnects after it.
var ErrStreamIdle = errors.New("no message or heartbeat")

// errNotHandled ends a stream attempt whose handler failed, so the reconnect
// replays from the hub queue in order.
var errNotHandled = errors.New("message not handled")

// WaitError is a handler failure that asks Listen to wait at least After
// before its next attempt: the hint a downstream Retry-After gave.
type WaitError struct {
	After time.Duration
	Err   error
}

func (e *WaitError) Error() string { return e.Err.Error() + "; wait " + e.After.String() }
func (e *WaitError) Unwrap() error { return e.Err }

// ListenOptions tune Listen. The zero value is the default.
type ListenOptions struct {
	// Idle is how long a stream may be silent before it counts as dead.
	// Zero means DefaultIdle.
	Idle time.Duration
	// OnReconnect hears each stream attempt that ended and the wait before
	// the next. err is nil when the hub closed the stream cleanly.
	OnReconnect func(err error, wait time.Duration)
	// Sleep, Jitter and Now are for tests. Nil means SleepCtx, a random
	// value in [d/2, d] and time.Now.
	Sleep  func(context.Context, time.Duration) error
	Jitter func(time.Duration) time.Duration
	Now    func() time.Time
}

// Listen subscribes and passes each delivered message to handle, in order,
// until ctx ends (nil) or the hub refuses the caller for good (the described
// error: EndsSession, or CALLER_NOT_ALLOWED for a token that may not
// subscribe). handle runs on ctx, never cut off by the idle watchdog, and
// acknowledges (Ack) what it is done with: delivery is at-least-once, and an
// unacknowledged message comes back on the next stream.
//
// A handle error ends the stream attempt and leaves the message pending, so
// it comes back first on reconnect and nothing behind it is handled before
// it. Listen reconnects after any other failure: jittered backoff from 1s to
// 5s, reset once a stream received anything, a minute after a refusal that
// does not heal on its own, never sooner than the hub's Retry-After (capped
// at an hour) or a handler's WaitError.
func (c *Client) Listen(ctx context.Context, handle func(context.Context, *sockv1.Message) error, o ListenOptions) error {
	if o.Idle <= 0 {
		o.Idle = DefaultIdle
	}
	if o.Sleep == nil {
		o.Sleep = SleepCtx
	}
	if o.Jitter == nil {
		o.Jitter = equalJitter
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	backoff := reconnectBase
	for {
		received, err := c.listenOnce(ctx, handle, o.Idle)
		if ctx.Err() != nil {
			return nil
		}
		if EndsSession(err) || Reason(err) == sockv1.ErrorReason_ERROR_REASON_CALLER_NOT_ALLOWED {
			return Describe(err)
		}
		if received && !errors.Is(err, errNotHandled) {
			backoff = reconnectBase // the connection worked; the next failure is fresh
		}
		if err != nil && !reconnectable(err) {
			backoff = refusalBackoff
		}
		wait := o.Jitter(backoff)
		if err != nil {
			if ra, ok := RetryAfter(err, o.Now()); ok {
				wait = max(wait, min(ra, maxRetryAfter)) // a floor jitter never cuts
			}
			if we, ok := errors.AsType[*WaitError](err); ok {
				wait = max(wait, we.After)
			}
		}
		if o.OnReconnect != nil {
			o.OnReconnect(Describe(err), wait)
		}
		if o.Sleep(ctx, wait) != nil {
			return nil
		}
		backoff = min(backoff*2, reconnectMax)
	}
}

// listenOnce holds one Subscribe stream and reports whether anything (a
// message or a heartbeat) arrived. A watchdog cancels the stream after idle
// with nothing received, so a half-open connection cannot hang it. It starts
// before Subscribe, which blocks until response headers arrive.
func (c *Client) listenOnce(ctx context.Context, handle func(context.Context, *sockv1.Message) error, idle time.Duration) (bool, error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var idled atomic.Bool
	watchdog := time.AfterFunc(idle, func() {
		idled.Store(true)
		cancel()
	})
	defer watchdog.Stop()
	idleErr := func() error { return fmt.Errorf("%w in %s", ErrStreamIdle, idle) }

	stream, err := c.Messaging.Subscribe(sctx, connect.NewRequest(&sockv1.SubscribeRequest{}))
	if err != nil {
		if idled.Load() {
			return false, idleErr()
		}
		return false, err
	}
	defer func() { _ = stream.Close() }()
	received := false
	for stream.Receive() {
		watchdog.Stop()
		received = true
		if m := stream.Msg().GetMessage(); m != nil {
			if err := handle(ctx, m); err != nil {
				return received, fmt.Errorf("%w (%s): %w", errNotHandled, m.GetId(), err)
			}
		}
		watchdog.Reset(idle)
	}
	if idled.Load() {
		return received, idleErr()
	}
	return received, stream.Err()
}

// reconnectable reports whether a stream failure can heal by itself soon.
// Refusals about the caller or what it sent, and Unimplemented, do not.
func reconnectable(err error) bool {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return true // network-level failure, the idle watchdog or a handler
	}
	switch ce.Code() {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeInvalidArgument, connect.CodeUnimplemented:
		return false
	}
	return true
}

// equalJitter returns a random duration in [d/2, d]: half the backoff is kept
// so a reconnect never degenerates into an immediate retry, and the rest
// spreads clients returning to a restarted hub.
func equalJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(d-half+1)
}
