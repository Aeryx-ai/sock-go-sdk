package sock

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
)

type fakeClock struct {
	now   time.Time
	waits []time.Duration
}

func (f *fakeClock) sleep(_ context.Context, d time.Duration) error {
	f.waits = append(f.waits, d)
	f.now = f.now.Add(d)
	return nil
}

func retrier(f *fakeClock) Retrier {
	return Retrier{Attempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second, Sleep: f.sleep, Now: func() time.Time { return f.now }, Jitter: func(d time.Duration) time.Duration { return d }}
}

func TestRetryBacksOffThenSucceeds(t *testing.T) {
	f := &fakeClock{now: time.Unix(0, 0)}
	calls := 0
	err := retrier(f).Do(context.Background(), func(context.Context) error {
		calls++
		if calls < 4 {
			return Unavailable()
		}
		return nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("err %v calls %d", err, calls)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	if len(f.waits) != len(want) {
		t.Fatalf("waits %v", f.waits)
	}
	for i := range want {
		if f.waits[i] != want[i] {
			t.Fatalf("waits %v, want %v", f.waits, want)
		}
	}
}

func TestRetryHonorsRetryAfterDate(t *testing.T) {
	f := &fakeClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	calls := 0
	err := retrier(f).Do(context.Background(), func(context.Context) error {
		calls++
		if calls == 1 {
			return RateLimited(f.now.Add(12 * time.Second).Format(http.TimeFormat))
		}
		return nil
	})
	if err != nil || len(f.waits) != 1 || f.waits[0] != 12*time.Second {
		t.Fatalf("err %v waits %v", err, f.waits)
	}
}

func TestRetryStopsOnPermanentError(t *testing.T) {
	f := &fakeClock{}
	calls := 0
	err := retrier(f).Do(context.Background(), func(context.Context) error {
		calls++
		return NotMember()
	})
	if err == nil || calls != 1 || len(f.waits) != 0 {
		t.Fatalf("err %v calls %d waits %v", err, calls, f.waits)
	}
}

func TestRetryGivesUpAfterAttempts(t *testing.T) {
	f := &fakeClock{}
	calls := 0
	err := retrier(f).Do(context.Background(), func(context.Context) error {
		calls++
		return Unavailable()
	})
	if err == nil || calls != 6 || len(f.waits) != 5 {
		t.Fatalf("err %v calls %d waits %v", err, calls, f.waits)
	}
	if f.waits[4] != 8*time.Second {
		t.Fatalf("waits %v", f.waits)
	}
}

func TestRetryCapsBackoff(t *testing.T) {
	f := &fakeClock{}
	r := retrier(f)
	r.Attempts = 10
	_ = r.Do(context.Background(), func(context.Context) error { return Unavailable() })
	for _, w := range f.waits {
		if w > 30*time.Second {
			t.Fatalf("wait %v exceeds cap", w)
		}
	}
}

func TestRetrierZeroValueIsSafe(t *testing.T) {
	calls := 0
	err := Retrier{}.Do(context.Background(), func(context.Context) error {
		calls++
		return Unavailable()
	})
	if err == nil || calls != 1 {
		t.Fatalf("err %v calls %d", err, calls)
	}
	calls = 0
	if err := (Retrier{}).Do(context.Background(), func(context.Context) error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatalf("err %v calls %d", err, calls)
	}
}

// A Retry-After longer than Max cannot be honored inside the budget: retrying
// sooner is what the header asks callers not to do (the owner-direct limits
// answer in minutes), so Do gives up at once and
// the caller sees the wait.
func TestRetryAfterBeyondMaxGivesUp(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for name, hdr := range map[string]string{
		"ten minutes":  "600",
		"huge seconds": "9223372036854775807",
		"far date":     now.AddDate(1000, 0, 0).Format(http.TimeFormat),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeClock{now: now}
			calls := 0
			err := retrier(f).Do(context.Background(), func(context.Context) error {
				calls++
				return RateLimited(hdr)
			})
			if err == nil || calls != 1 || len(f.waits) != 0 {
				t.Fatalf("err %v calls %d waits %v", err, calls, f.waits)
			}
		})
	}
}

// within Max the wait is the server's, then the retry goes through
func TestRetryAfterWithinMaxIsHonored(t *testing.T) {
	f := &fakeClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	calls := 0
	err := retrier(f).Do(context.Background(), func(context.Context) error {
		calls++
		if calls == 1 {
			return RateLimited("20")
		}
		return nil
	})
	if err != nil || len(f.waits) != 1 || f.waits[0] != 20*time.Second {
		t.Fatalf("err %v waits %v", err, f.waits)
	}
}

func TestRetryZeroMaxDefaultsTo30s(t *testing.T) {
	f := &fakeClock{}
	r := retrier(f)
	r.Max = 0
	r.Attempts = 10
	_ = r.Do(context.Background(), func(context.Context) error { return Unavailable() })
	if len(f.waits) != 9 || f.waits[8] != 30*time.Second {
		t.Fatalf("waits %v", f.waits)
	}
}

// Retry-After is a floor: "0" or a past date never cuts the backoff to nothing
func TestRetryAfterBelowBackoffKeepsTheBackoff(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for name, hdr := range map[string]string{"zero": "0", "past date": now.Add(-time.Hour).Format(http.TimeFormat)} {
		t.Run(name, func(t *testing.T) {
			f := &fakeClock{now: now}
			calls := 0
			_ = retrier(f).Do(context.Background(), func(context.Context) error {
				calls++
				if calls < 3 {
					return RateLimited(hdr)
				}
				return nil
			})
			if len(f.waits) != 2 || f.waits[0] != 500*time.Millisecond || f.waits[1] != time.Second {
				t.Fatalf("waits %v", f.waits)
			}
		})
	}
}

// a zero Base backs off from 500ms, and the default jitter keeps each wait in
// the upper half of its step
func TestRetryZeroBaseBacksOffWithJitter(t *testing.T) {
	f := &fakeClock{}
	_ = Retrier{Attempts: 4, Sleep: f.sleep}.Do(context.Background(), func(context.Context) error { return Unavailable() })
	steps := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	if len(f.waits) != len(steps) {
		t.Fatalf("waits %v", f.waits)
	}
	for i, d := range steps {
		if f.waits[i] < d/2 || f.waits[i] > d {
			t.Fatalf("waits %v, want each in [d/2, d] of %v", f.waits, steps)
		}
	}
}

// ctx ending during a backoff still reports what the last attempt hit
func TestRetryCanceledDuringBackoffKeepsTheLastError(t *testing.T) {
	r := Retrier{Attempts: 3, Sleep: func(context.Context, time.Duration) error { return context.Canceled }}
	err := r.Do(context.Background(), func(context.Context) error { return RateLimited("1") })
	if !errors.Is(err, context.Canceled) || Reason(err) != sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED || !Transient(err) {
		t.Fatalf("err %v reason %v", err, Reason(err))
	}
	if got := Describe(err).Error(); !strings.Contains(got, "sending too fast") {
		t.Fatalf("Describe = %q", got)
	}
}
