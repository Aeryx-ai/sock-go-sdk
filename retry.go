package sock

import (
	"context"
	"time"
)

// Retrier retries transient failures with exponential backoff. A server's
// Retry-After is a floor under the computed delay.
type Retrier struct {
	Attempts int
	Base     time.Duration
	Max      time.Duration
	Sleep    func(context.Context, time.Duration) error
	Now      func() time.Time
}

// DefaultRetrier returns 6 attempts, 500ms doubling, capped at 30s.
func DefaultRetrier() Retrier {
	return Retrier{Attempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second, Sleep: SleepCtx, Now: time.Now}
}

// SleepCtx waits d or until ctx is done.
func SleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Do calls fn until it succeeds, fails permanently, or attempts run out.
// Attempts below 1 count as 1; nil Sleep and Now default to SleepCtx and
// time.Now, and a zero Max to 30s, so the zero value is safe. A Retry-After
// longer than Max ends Do at once with the error: it is never retried early.
func (r Retrier) Do(ctx context.Context, fn func(context.Context) error) error {
	attempts := max(r.Attempts, 1)
	sleep, now := r.Sleep, r.Now
	if sleep == nil {
		sleep = SleepCtx
	}
	if now == nil {
		now = time.Now
	}
	maxWait := r.Max
	if maxWait <= 0 {
		maxWait = 30 * time.Second
	}
	delay := min(r.Base, maxWait)
	var err error
	for attempt := range attempts {
		err = fn(ctx)
		if err == nil || !Transient(err) {
			return err
		}
		if attempt == attempts-1 {
			break
		}
		wait := delay
		if ra, ok := RetryAfter(err, now()); ok {
			if ra > maxWait {
				// retrying sooner is what the header asks not to do: give
				// up, and the caller reports the wait
				return err
			}
			wait = max(wait, ra) // a floor: "0" or a past date never cuts the backoff
		}
		if serr := sleep(ctx, wait); serr != nil {
			return serr
		}
		delay = min(delay*2, maxWait)
	}
	return err
}
