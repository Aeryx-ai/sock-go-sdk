package sock

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
)

func TestTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", Unavailable(), true},
		{"deadline", connect.NewError(connect.CodeDeadlineExceeded, errors.New("slow")), true},
		{"rate limited", RateLimited("1"), true},
		{"other exhausted", connect.NewError(connect.CodeResourceExhausted, errors.New("x")), false},
		{"not member", NotMember(), false},
		{"plain", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Transient(c.err); got != c.want {
				t.Fatalf("Transient = %v, want %v", got, c.want)
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	d, ok := RetryAfter(RateLimited("7"), now)
	if !ok || d != 7*time.Second {
		t.Fatalf("seconds form: %v %v", d, ok)
	}
	d, ok = RetryAfter(RateLimited(now.Add(90*time.Second).Format(http.TimeFormat)), now)
	if !ok || d != 90*time.Second {
		t.Fatalf("date form: %v %v", d, ok)
	}
	d, ok = RetryAfter(RateLimited(now.Add(-time.Minute).Format(http.TimeFormat)), now)
	if !ok || d != 0 {
		t.Fatalf("past date: %v %v", d, ok)
	}
	// RFC 9110 obsolete forms a recipient must accept: RFC 850 and asctime
	for _, hdr := range []string{"Saturday, 10-Oct-26 12:01:30 GMT", "Sat Oct 10 12:01:30 2026"} {
		if d, ok := RetryAfter(RateLimited(hdr), now); !ok || d != 90*time.Second {
			t.Fatalf("%q: %v %v", hdr, d, ok)
		}
	}
	if _, ok := RetryAfter(RateLimited("soon"), now); ok {
		t.Fatal("garbage header must be ignored")
	}
	if _, ok := RetryAfter(Unavailable(), now); ok {
		t.Fatal("no header means no Retry-After")
	}
}

func TestDescribeUsesReason(t *testing.T) {
	got := Describe(NotMember())
	if got == nil || got.Error() != "you are not a member of that conversation" {
		t.Fatalf("Describe = %v", got)
	}
}

func TestDescribeEmptyMessageFallsBackToCode(t *testing.T) {
	for name, err := range map[string]error{
		"nil cause":   connect.NewError(connect.CodeUnavailable, nil),
		"empty cause": connect.NewError(connect.CodeUnavailable, errors.New("")),
	} {
		t.Run(name, func(t *testing.T) {
			if got := Describe(err); got == nil || got.Error() != "unavailable" {
				t.Fatalf("Describe = %q, want %q", got, "unavailable")
			}
		})
	}
}

func TestDescribeKeepsOriginalClassifiable(t *testing.T) {
	err := Describe(RateLimited("1"))
	if !Transient(err) || Reason(err) == 0 {
		t.Fatalf("Describe hid the original error: transient=%v reason=%v", Transient(err), Reason(err))
	}
	if Transient(Describe(NotMember())) {
		t.Fatal("permanent error became transient")
	}
}

// reasons that need no command name to act on are worded here, so every
// caller says the same thing
func TestDescribeWordsTheReasonsEveryCallerMeets(t *testing.T) {
	for reason, want := range map[sockv1.ErrorReason]string{
		sockv1.ErrorReason_ERROR_REASON_HANDLE_TAKEN:          "that handle is taken",
		sockv1.ErrorReason_ERROR_REASON_PUPPET_NAME_TAKEN:     "you already have a puppet with that name",
		sockv1.ErrorReason_ERROR_REASON_INVITE_INVALID:        "the invite code is invalid, expired or already used",
		sockv1.ErrorReason_ERROR_REASON_NOT_OWNER:             "you do not own that puppet",
		sockv1.ErrorReason_ERROR_REASON_GROUP_FULL:            "the group is full",
		sockv1.ErrorReason_ERROR_REASON_OWNER_NOT_MEMBER:      "a puppet's owner must be a member of the conversation too",
		sockv1.ErrorReason_ERROR_REASON_DIRECT_IMMUTABLE:      "a direct conversation's members cannot change",
		sockv1.ErrorReason_ERROR_REASON_PEER_NOT_ALLOWED:      "a direct conversation needs a human or one of your own puppets",
		sockv1.ErrorReason_ERROR_REASON_SUBSCRIPTION_REQUIRED: "this needs Sock Pro; subscribe in the sock app",
	} {
		err := Describe(withReason(connect.NewError(connect.CodeFailedPrecondition, nil), reason))
		if err == nil || err.Error() != want {
			t.Errorf("%v: %v, want %q", reason, err, want)
		}
	}
}

func TestDescribeLimitReachedWithoutAMessageStillSaysWhichKind(t *testing.T) {
	err := withReason(connect.NewError(connect.CodeFailedPrecondition, nil), sockv1.ErrorReason_ERROR_REASON_LIMIT_REACHED)
	if got := Describe(err).Error(); !strings.HasPrefix(got, "an account limit is reached; erase") {
		t.Fatalf("Describe = %q", got)
	}
}

// a rate limit says how long to wait, so a hook or a model can tell the user
func TestDescribeRateLimitedNamesTheWait(t *testing.T) {
	got := Describe(RateLimited("600")).Error()
	if !strings.Contains(got, "10m0s") || strings.Contains(got, "after retries") {
		t.Fatalf("Describe = %q", got)
	}
	if got := Describe(RateLimited("")).Error(); got == "" || strings.Contains(got, "after retries") {
		t.Fatalf("Describe without a header = %q", got)
	}
}
