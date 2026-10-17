package sock

import (
	"context"
	"net/http"
	"testing"
	"time"
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
	return Retrier{Attempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second, Sleep: f.sleep, Now: func() time.Time { return f.now }}
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
// sooner is what the header asks callers not to do (ADR 20's owner-direct
// limits answer in minutes), so Do gives up at once and the caller sees the
// wait.
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
