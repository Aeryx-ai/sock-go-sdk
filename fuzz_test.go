package sock

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

var tokenPrefixes = map[string]TokenKind{
	"sock_st_": TokenSession,
	"sock_pt_": TokenPuppet,
	"sock_mk_": TokenMachineKey,
}

func FuzzKindOf(f *testing.F) {
	for _, s := range []string{"", "sock_st_abc", "sock_pt_dev_secret", "sock_mk_x", "sock_xx_y", "SOCK_PT_x", " sock_pt_x", "sock_pt", "sock_pt_ x\n"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got := KindOf(s)
		want := TokenUnknown
		for p, k := range tokenPrefixes {
			if strings.HasPrefix(s, p) {
				want = k
			}
		}
		if got != want {
			t.Fatalf("KindOf(%q) = %v, want %v", s, got, want)
		}
	})
}

func FuzzValidate(f *testing.F) {
	f.Add("https://api.joinsock.com", "sock_pt_abcdef123456", "")
	f.Add("http://127.0.0.1:8080", "sock_st_abcdef123456", "gate")
	f.Add("http://localhost/x", "sock_mk_abcdef123456", "")
	f.Add("http://evil.example/?k=supersecretquery", "sock_pt_abcdef123456", "")
	f.Add("https://user:hunter2hunter2@host", "sock_pt_abcdef123456", "")
	f.Add("user:hunter2hunter2@host", "sock_pt_abcdef123456", "")
	f.Add("https://host", "sock_pt_abc def", "")
	f.Add("https://host", "sock_pt_abc", "ga te\n")
	f.Add("ftp://host/x", "sock_xx_abcdef123456", "")
	f.Add("https://[::1", "sock_pt_abcdef123456", "")
	f.Fuzz(func(t *testing.T, rawURL, token, gate string) {
		err := Validate(Config{URL: rawURL, Token: token, AccessGate: gate})
		if err != nil {
			msg := err.Error()
			// Error text never echoes the token, nor any part of the URL
			// beyond scheme and host. Short values can match fixed text, so
			// only distinctive ones are checked.
			if len(token) >= 12 && !strings.Contains(rawURL, token) && strings.Contains(msg, token) {
				t.Fatalf("error echoes the token: %q", msg)
			}
			if u, perr := url.Parse(rawURL); perr == nil {
				if p, ok := u.User.Password(); ok && len(p) >= 8 && strings.Contains(msg, p) {
					t.Fatalf("error echoes the URL password: %q", msg)
				}
				if q := u.RawQuery; len(q) >= 8 && !strings.Contains(u.Host, q) && !strings.Contains(u.Scheme, q) && strings.Contains(msg, q) {
					t.Fatalf("error echoes the URL query: %q", msg)
				}
			}
			return
		}
		if strings.ContainsFunc(token, unicode.IsSpace) {
			t.Fatalf("accepted a token with whitespace: %q", token)
		}
		if KindOf(token) == TokenUnknown {
			t.Fatalf("accepted a token of unknown kind: %q", token)
		}
		if strings.ContainsFunc(gate, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			t.Fatalf("accepted an access gate with whitespace or a control character: %q", gate)
		}
		u, perr := url.Parse(rawURL)
		if perr != nil {
			t.Fatalf("accepted an unparsable URL %q: %v", rawURL, perr)
		}
		if u.User != nil || u.Host == "" || u.Opaque != "" {
			t.Fatalf("accepted URL %q with user, opaque part or no host", rawURL)
		}
		if u.Scheme != "https" && (u.Scheme != "http" || !isLoopback(u.Hostname())) {
			t.Fatalf("accepted non-https URL %q off loopback", rawURL)
		}
	})
}

func FuzzRetryAfter(f *testing.F) {
	for _, s := range []string{"", "0", "1", "120", " 7 ", "-1", "+5", "1.5", "99999999999999999999", "9223372036854775807",
		"Wed, 21 Oct 2015 07:28:00 GMT", "Mon, 02 Jan 2006 15:04:05 GMT", "Thu, 01 Jan 1970 00:00:00 GMT", "garbage"} {
		f.Add(s, int64(1_700_000_000))
	}
	f.Fuzz(func(t *testing.T, header string, nowUnix int64) {
		now := time.Unix(nowUnix%(1<<40), 0).UTC()
		ce := connect.NewError(connect.CodeUnavailable, errors.New("x"))
		ce.Meta().Set("Retry-After", header)
		d, ok := RetryAfter(ce, now)
		if d < 0 {
			t.Fatalf("RetryAfter(%q) = %v, negative", header, d)
		}
		if !ok && d != 0 {
			t.Fatalf("RetryAfter(%q) = %v with ok false", header, d)
		}
		v := strings.TrimSpace(header)
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			want := time.Duration(min(n, maxRetryAfterSecs)) * time.Second
			if !ok || d != want {
				t.Fatalf("RetryAfter(%q) = %v, %v; want %v", header, d, ok, want)
			}
		}
		if at, err := time.Parse(http.TimeFormat, v); err == nil && !ok {
			t.Fatalf("RetryAfter(%q) refused a valid HTTP date %v", header, at)
		}
		// Not a Connect error: nothing to read.
		if d, ok := RetryAfter(errors.New(header), now); ok || d != 0 {
			t.Fatalf("RetryAfter(plain error) = %v, %v", d, ok)
		}
	})
}

// FuzzRetryAfterDateRoundTrip formats a future instant as an HTTP date and
// reads it back as the wait until then.
func FuzzRetryAfterDateRoundTrip(f *testing.F) {
	f.Add(int64(1_700_000_000), int64(0))
	f.Add(int64(1_700_000_000), int64(90))
	f.Add(int64(0), int64(-5))
	f.Fuzz(func(t *testing.T, nowUnix, delta int64) {
		now := time.Unix(nowUnix%(1<<34), 0).UTC()
		delta %= 1 << 31
		at := now.Add(time.Duration(delta) * time.Second)
		ce := connect.NewError(connect.CodeUnavailable, errors.New("x"))
		ce.Meta().Set("Retry-After", at.Format(http.TimeFormat))
		d, ok := RetryAfter(ce, now)
		if at.Year() < 0 || at.Year() > 9999 {
			return // outside what the date form can carry
		}
		want := time.Duration(max(delta, 0)) * time.Second
		if !ok || d != want {
			t.Fatalf("RetryAfter(%s) at %v = %v, %v; want %v", at.Format(http.TimeFormat), now, d, ok, want)
		}
	})
}

func FuzzDescribe(f *testing.F) {
	f.Add(uint32(connect.CodeResourceExhausted), int32(sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED), "slow down", "30")
	f.Add(uint32(connect.CodeInvalidArgument), int32(sockv1.ErrorReason_ERROR_REASON_FIELD_INVALID), "text too long", "")
	f.Add(uint32(connect.CodeFailedPrecondition), int32(sockv1.ErrorReason_ERROR_REASON_LIMIT_REACHED), "", "")
	f.Add(uint32(connect.CodeUnavailable), int32(0), "", "")
	f.Add(uint32(0), int32(9999), "msg", "x")
	f.Add(uint32(99), int32(-1), "", "")
	f.Fuzz(func(t *testing.T, code uint32, reason int32, msg, retryAfter string) {
		var cause error
		if msg != "" {
			cause = errors.New(msg)
		}
		ce := connect.NewError(connect.Code(code), cause)
		if reason != 0 {
			ce = withReason(ce, sockv1.ErrorReason(reason))
		}
		if retryAfter != "" {
			ce.Meta().Set("Retry-After", retryAfter)
		}
		got := Describe(ce)
		if got == nil {
			t.Fatal("Describe(non-nil) = nil")
		}
		if got.Error() == "" {
			t.Fatal("Describe returned empty text")
		}
		var back *connect.Error
		if !errors.As(got, &back) || back != ce {
			t.Fatal("described error does not unwrap to the original")
		}
		if Reason(got) != Reason(ce) || Transient(got) != Transient(ce) {
			t.Fatal("Describe changed the classification")
		}
		if d, ok := RetryAfter(got, time.Now()); d < 0 || (!ok && d != 0) {
			t.Fatalf("RetryAfter(described) = %v, %v", d, ok)
		}
	})
}
