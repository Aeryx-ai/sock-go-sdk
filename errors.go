package sock

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
)

// maxRetryAfterSecs bounds parsed Retry-After seconds (about 31 years).
const maxRetryAfterSecs = 1_000_000_000

// Reason returns the ErrorInfo reason attached to err, or UNSPECIFIED.
func Reason(err error) sockv1.ErrorReason {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return sockv1.ErrorReason_ERROR_REASON_UNSPECIFIED
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			continue
		}
		if info, ok := v.(*sockv1.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return sockv1.ErrorReason_ERROR_REASON_UNSPECIFIED
}

// Transient reports whether retrying the same request can succeed.
func Transient(err error) bool {
	if err == nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		return true
	case connect.CodeResourceExhausted:
		return Reason(err) == sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED
	}
	return false
}

// RetryAfter reads the Retry-After header from a Connect error, in either
// delta-seconds or any HTTP-date form. A date in the past means retry now.
func RetryAfter(err error, now time.Time) (time.Duration, bool) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return 0, false
	}
	v := strings.TrimSpace(ce.Meta().Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if secs, perr := strconv.ParseInt(v, 10, 64); perr == nil && secs >= 0 {
		// Clamp before multiplying so huge values cannot overflow Duration.
		return time.Duration(min(secs, maxRetryAfterSecs)) * time.Second, true
	}
	if at, perr := http.ParseTime(v); perr == nil {
		return max(at.Sub(now), 0), true
	}
	return 0, false
}

var reasonText = map[sockv1.ErrorReason]string{
	sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID:        "the server rejected the token; mint a new one in the sock app",
	sockv1.ErrorReason_ERROR_REASON_CALLER_NOT_ALLOWED:   "this token cannot do that (puppet and human tokens allow different calls)",
	sockv1.ErrorReason_ERROR_REASON_NOT_A_MEMBER:         "you are not a member of that conversation",
	sockv1.ErrorReason_ERROR_REASON_NOT_FOUND:            "not found",
	sockv1.ErrorReason_ERROR_REASON_FIELD_INVALID:        "the server rejected a field",
	sockv1.ErrorReason_ERROR_REASON_IDEMPOTENCY_MISMATCH: "idempotency key reused with a different message",
	sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED:         "sending too fast",
	sockv1.ErrorReason_ERROR_REASON_ACCOUNT_SUSPENDED:    "this account is suspended",
	sockv1.ErrorReason_ERROR_REASON_TERMS_NOT_ACCEPTED:   "the owner has not accepted the current terms; accept them in the sock app",
	// after the server's message, which names the limit: puppets per owner, tokens per puppet or machine keys
	// per human
	sockv1.ErrorReason_ERROR_REASON_LIMIT_REACHED: "erase a puppet you no longer use or revoke an unused token or machine key, then try again",
	// the reasons that need no command to act on, worded once for every caller
	sockv1.ErrorReason_ERROR_REASON_HANDLE_TAKEN:          "that handle is taken",
	sockv1.ErrorReason_ERROR_REASON_PUPPET_NAME_TAKEN:     "you already have a puppet with that name",
	sockv1.ErrorReason_ERROR_REASON_INVITE_INVALID:        "the invite code is invalid, expired or already used",
	sockv1.ErrorReason_ERROR_REASON_NOT_OWNER:             "you do not own that puppet",
	sockv1.ErrorReason_ERROR_REASON_GROUP_FULL:            "the group is full",
	sockv1.ErrorReason_ERROR_REASON_OWNER_NOT_MEMBER:      "a puppet's owner must be a member of the conversation too",
	sockv1.ErrorReason_ERROR_REASON_DIRECT_IMMUTABLE:      "a direct conversation's members cannot change",
	sockv1.ErrorReason_ERROR_REASON_PEER_NOT_ALLOWED:      "a direct conversation needs a human or one of your own puppets",
	sockv1.ErrorReason_ERROR_REASON_SUBSCRIPTION_REQUIRED: "this needs Sock Pro; subscribe in the sock app",
}

// described is Describe's result: the actionable text, with the original
// error still reachable so callers can classify it with Transient or Reason.
type described struct {
	text string
	err  error
}

func (d *described) Error() string { return d.text }
func (d *described) Unwrap() error { return d.err }

// Describe turns a server error into a message a person can act on, keeping
// the server's detail for field errors.
func Describe(err error) error {
	if err == nil {
		return nil
	}
	r := Reason(err)
	text, ok := reasonText[r]
	if !ok {
		var ce *connect.Error
		if errors.As(err, &ce) {
			return &described{message(ce), err}
		}
		return err
	}
	var ce *connect.Error
	switch {
	case r == sockv1.ErrorReason_ERROR_REASON_FIELD_INVALID && errors.As(err, &ce):
		return &described{text + ": " + message(ce), err}
	case r == sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED:
		if wait, ok := RetryAfter(err, time.Now()); ok {
			return &described{text + "; try again in " + wait.Round(time.Second).String(), err}
		}
		return &described{text + "; try again later", err}
	case r == sockv1.ErrorReason_ERROR_REASON_LIMIT_REACHED:
		limit := "an account limit is reached"
		if errors.As(err, &ce) && ce.Message() != "" {
			limit = ce.Message()
		}
		return &described{limit + "; " + text, err}
	}
	return &described{text, err}
}

// message is the server's text, or the code name when it sent none, so
// output is never a bare "sock: ".
func message(ce *connect.Error) string {
	if m := ce.Message(); m != "" {
		return m
	}
	return ce.Code().String()
}

// EndsSession reports whether err means the token is rejected or the account
// suspended: no retry or reconnect helps, and the caller signs out or needs a
// new token.
func EndsSession(err error) bool {
	switch Reason(err) {
	case sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID, sockv1.ErrorReason_ERROR_REASON_ACCOUNT_SUSPENDED:
		return true
	}
	return false
}

// AlreadyDone reports an answer that means a retried change already happened:
// the membership, puppet or token is gone.
func AlreadyDone(err error) bool {
	switch Reason(err) {
	case sockv1.ErrorReason_ERROR_REASON_NOT_A_MEMBER, sockv1.ErrorReason_ERROR_REASON_NOT_FOUND:
		return true
	}
	return false
}
