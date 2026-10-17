package sock

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
	"github.com/guygrigsby/sock-contract/gen/go/sock/v1/sockv1connect"
)

const token = "sock_pt_good"

type identity struct {
	sockv1connect.UnimplementedIdentityServiceHandler
	ua, auth atomic.Value
}

func (i *identity) GetMe(_ context.Context, r *connect.Request[sockv1.GetMeRequest]) (*connect.Response[sockv1.GetMeResponse], error) {
	i.ua.Store(r.Header().Get("User-Agent"))
	i.auth.Store(r.Header().Get("Authorization"))
	if r.Header().Get("Authorization") != "Bearer "+token {
		return nil, withReason(connect.NewError(connect.CodeUnauthenticated, nil), sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID)
	}
	return connect.NewResponse(&sockv1.GetMeResponse{Account: &sockv1.Account{Id: "puppet-1"}}), nil
}

func serve(t *testing.T) (*identity, string) {
	t.Helper()
	h := &identity{}
	mux := http.NewServeMux()
	p, hh := sockv1connect.NewIdentityServiceHandler(h)
	mux.Handle(p, hh)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return h, srv.URL
}

func TestNewSendsTokenAndUserAgent(t *testing.T) {
	h, url := serve(t)
	c := New(Config{URL: url, Token: token}, "sock-test/1.2.3")
	res, err := c.Identity.GetMe(t.Context(), connect.NewRequest(&sockv1.GetMeRequest{}))
	if err != nil || res.Msg.GetAccount().GetId() != "puppet-1" {
		t.Fatalf("GetMe: %v %v", res, err)
	}
	if h.ua.Load() != "sock-test/1.2.3" {
		t.Fatalf("user agent %v", h.ua.Load())
	}
}

func TestBadTokenReason(t *testing.T) {
	_, url := serve(t)
	c := New(Config{URL: url, Token: "sock_pt_wrong"}, "t")
	_, err := c.Identity.GetMe(t.Context(), connect.NewRequest(&sockv1.GetMeRequest{}))
	if Reason(err) != sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID {
		t.Fatalf("reason %v from %v", Reason(err), err)
	}
	if Transient(err) {
		t.Fatal("token rejection must not be transient")
	}
}

func TestTokenNotSentOnRedirect(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
	}))
	t.Cleanup(other.Close)
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(first.Close)
	c := New(Config{URL: first.URL, Token: token}, "t")
	if _, err := c.Identity.GetMe(t.Context(), connect.NewRequest(&sockv1.GetMeRequest{})); err == nil {
		t.Fatal("redirect must surface as an error")
	}
	if leaked.Load() {
		t.Fatal("token reached the redirect target")
	}
}

func TestCallRetriesAndDescribes(t *testing.T) {
	calls := 0
	r := Retrier{Attempts: 3, Sleep: func(context.Context, time.Duration) error { return nil }}
	out, err := Call(t.Context(), r, func(context.Context) (string, error) {
		calls++
		if calls < 3 {
			return "", Unavailable()
		}
		return "ok", nil
	})
	if err != nil || out != "ok" || calls != 3 {
		t.Fatalf("out=%q err=%v calls=%d", out, err, calls)
	}
	_, err = Call(t.Context(), r, func(context.Context) (string, error) { return "", NotMember() })
	if err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("err = %v", err)
	}
}

// A timed-out attempt may have landed: Call retries it only for an
// idempotent call, so a create never runs twice.
func TestCallRetriesDeadlineExceededOnlyWhenIdempotent(t *testing.T) {
	for _, idempotent := range []bool{false, true} {
		calls := 0
		r := Retrier{Attempts: 3, Sleep: func(context.Context, time.Duration) error { return nil }, Idempotent: idempotent}
		_, _ = Call(t.Context(), r, func(context.Context) (string, error) {
			calls++
			return "", connect.NewError(connect.CodeDeadlineExceeded, nil)
		})
		if want := map[bool]int{false: 1, true: 3}[idempotent]; calls != want {
			t.Fatalf("idempotent %v: %d calls, want %d", idempotent, calls, want)
		}
	}
}

func TestValidate(t *testing.T) {
	good := "sock_pt_abc"
	tests := []struct{ name, url, token, want string }{
		{"ok", DefaultURL, good, ""},
		{"loopback http", "http://127.0.0.1:8080", good, ""},
		{"no url", "", good, "no server URL"},
		{"no token", DefaultURL, "", "no token"},
		{"inner space", DefaultURL, "sock_pt_a b", "whitespace"},
		{"bad prefix", DefaultURL, "sock_ot_1_2", "must start with"},
		{"remote http", "http://example.com", good, "must use https"},
		{"url credentials", "https://u:hunter2@example.com", good, "user or password"},
		{"scheme-less credentials", "user:hunter2@example.com", good, "not a valid absolute"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(Config{URL: tc.url, Token: tc.token})
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("error leaks credentials: %v", err)
			}
		})
	}
}

func TestValidateRefusesAGateWithWhitespaceOrControlsWithoutEchoingIt(t *testing.T) {
	for _, g := range []string{"a b", "a\nb", "a\x00b", "a\tb"} {
		err := Validate(Config{URL: "https://api.example", Token: token, AccessGate: g})
		if err == nil || strings.Contains(err.Error(), g) {
			t.Fatalf("gate %q: %v", g, err)
		}
	}
	if err := Validate(Config{URL: "https://api.example", Token: token, AccessGate: "gate-1"}); err != nil {
		t.Fatal(err)
	}
}

// The default is the production hub's joinsock.com host.
func TestDefaultURLIsProductionHub(t *testing.T) {
	if DefaultURL != "https://api.joinsock.com" {
		t.Fatalf("DefaultURL = %q, want https://api.joinsock.com", DefaultURL)
	}
}
