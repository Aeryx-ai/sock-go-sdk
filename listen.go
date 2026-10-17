package sock

import (
	"context"
	"errors"
	"fmt"
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

// HandlerError is a handler failure Listen gave up on: the message is still
// pending, and redelivering it alone would only fail again.
type HandlerError struct {
	MessageID string
	Err       error
}

func (e *HandlerError) Error() string { return "message " + e.MessageID + ": " + e.Err.Error() }
func (e *HandlerError) Unwrap() error { return e.Err }

// WaitError is a handler failure that asks Listen to redeliver the message
// no sooner than After: the hint a downstream Retry-After gave.
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
	// the next. err is nil when the hub closed the stream cleanly, and a
	// *HandlerError when a message is being redelivered.
	OnReconnect func(err error, wait time.Duration)
	// Sleep, Jitter and Now are for tests. Nil means SleepCtx, a random
	// value in [d/2, d] and time.Now.
	Sleep  func(context.Context, time.Duration) error
	Jitter func(time.Duration) time.Duration
	Now    func() time.Time
}

// Listen subscribes and passes each delivered message to handle, in order,
// until ctx ends (nil), the hub refuses the caller for good (the described
// error: EndsSession, or CALLER_NOT_ALLOWED for a token that may not
// subscribe) or handle fails for good (a *HandlerError). handle runs on ctx, never cut off by the idle watchdog, and
// acknowledges (Ack) what it is done with: delivery is at-least-once, and an
// unacknowledged message comes back on the next stream.
//
// A handle error is never read as the hub refusing the stream. It leaves the
// message pending, and nothing behind it is handled before it. When the error
// can heal (a WaitError, or a hub error Transient reports) the stream attempt
// ends and the message comes back first on reconnect, after the backoff below.
// Any other handle error would fail again on every redelivery, so Listen
// returns it as a *HandlerError naming the message: the caller fixes the
// cause, or Acks the message to skip it, and calls Listen again. A handler
// that wants a failed message retried later returns a WaitError.
//
// Listen reconnects after any stream failure: jittered backoff from 1s to 5s,
// reset once a stream received anything, a minute after a refusal that does
// not heal on its own, never sooner than the hub's Retry-After (capped at an
// hour). A redelivery backs off the same way, never sooner than the
// handler's Retry-After or WaitError.
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
		heard := Describe(err)
		if he, ok := errors.AsType[*HandlerError](err); ok {
			// the handler's own error, so no stream classification applies
			if _, retry := errors.AsType[*WaitError](he.Err); !retry && !Transient(he.Err) {
				return he
			}
			heard = he
		} else {
			if EndsSession(err) || Reason(err) == sockv1.ErrorReason_ERROR_REASON_CALLER_NOT_ALLOWED {
				return heard
			}
			if received {
				backoff = reconnectBase // the connection worked; the next failure is fresh
			}
			if err != nil && !reconnectable(err) {
				backoff = refusalBackoff
			}
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
			o.OnReconnect(heard, wait)
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
				return received, &HandlerError{MessageID: m.GetId(), Err: err}
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
		return true // network-level failure or the idle watchdog
	}
	switch ce.Code() {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeInvalidArgument, connect.CodeUnimplemented:
		return false
	}
	return true
}
