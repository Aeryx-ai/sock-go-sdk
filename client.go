// Package sock is the Go SDK for the sock hub: an authenticated Connect
// client, retries that honor Retry-After, actionable error text, and the
// messaging loops every client needs (Listen, Drain, Send, Ack).
package sock

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"connectrpc.com/connect"

	"github.com/aeryx-ai/sock-contract/gen/go/sock/v1/sockv1connect"
)

// UnaryTimeout bounds each attempt of a unary call; Subscribe has no deadline.
const UnaryTimeout = 30 * time.Second

// AccessGateHeader carries the staging access gate.
const AccessGateHeader = "Sock-Access-Gate"

// Client is the hub's services plus the retry policy the helper methods
// (Send, NotifyOwner, Ack, Drain) use.
type Client struct {
	Identity   sockv1connect.IdentityServiceClient
	Messaging  sockv1connect.MessagingServiceClient
	Moderation sockv1connect.ModerationServiceClient
	Operator   sockv1connect.OperatorServiceClient
	Retrier    Retrier
}

// Option adjusts the client New builds.
type Option func(*options)

type options struct {
	wrap func(http.RoundTripper) http.RoundTripper
	rpc  []connect.ClientOption
}

// WithTransport wraps the base transport, beneath the one that adds the
// token, gate and user agent: instrumentation sees each request as sent.
func WithTransport(wrap func(http.RoundTripper) http.RoundTripper) Option {
	return func(o *options) { o.wrap = wrap }
}

// WithConnectOptions passes opts (an interceptor, say) to every service.
func WithConnectOptions(opts ...connect.ClientOption) Option {
	return func(o *options) { o.rpc = append(o.rpc, opts...) }
}

// New builds a client that sends cfg.Token (when there is one: sign-in and
// registration run before a token exists) and cfg.AccessGate (when set) to
// the configured scheme and host only, and userAgent everywhere. New does not
// validate cfg; callers that need a token call Validate first. No client-wide
// timeout: Subscribe streams indefinitely. Redirects are never followed. The
// Retrier is DefaultRetrier.
func New(cfg Config, userAgent string, opts ...Option) *Client {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	rt := http.RoundTripper(newTransport(defaultLiveness))
	if o.wrap != nil {
		rt = o.wrap(rt)
	}
	base, _ := url.Parse(cfg.URL) // Validate checked the URL; a nil-safe zero URL matches nothing
	if base == nil {
		base = &url.URL{}
	}
	hc := &http.Client{
		Transport: &transport{
			base:   rt,
			token:  cfg.Token,
			gate:   cfg.AccessGate,
			ua:     userAgent,
			scheme: base.Scheme,
			host:   base.Host,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &Client{
		Identity:   sockv1connect.NewIdentityServiceClient(hc, cfg.URL, o.rpc...),
		Messaging:  sockv1connect.NewMessagingServiceClient(hc, cfg.URL, o.rpc...),
		Moderation: sockv1connect.NewModerationServiceClient(hc, cfg.URL, o.rpc...),
		Operator:   sockv1connect.NewOperatorServiceClient(hc, cfg.URL, o.rpc...),
		Retrier:    DefaultRetrier(),
	}
}

type transport struct {
	base         http.RoundTripper
	token, gate  string
	ua           string
	scheme, host string
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if r.URL.Scheme == t.scheme && r.URL.Host == t.host {
		if t.token != "" {
			r.Header.Set("Authorization", "Bearer "+t.token)
		}
		if t.gate != "" {
			r.Header.Set(AccessGateHeader, t.gate)
		}
	}
	r.Header.Set("User-Agent", t.ua)
	return t.base.RoundTrip(r)
}

// Call runs one unary RPC under r with a deadline per attempt. The error is
// already actionable text (see Describe). An attempt that times out is
// retried only when r.Idempotent is set, since it may have landed: set it for
// reads and for writes the hub dedupes (an idempotency key), never for a
// create that would run twice.
func Call[T any](ctx context.Context, r Retrier, fn func(context.Context) (T, error)) (T, error) {
	var out T
	err := r.Do(ctx, func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, UnaryTimeout)
		defer cancel()
		var err error
		out, err = fn(ctx)
		return err
	})
	return out, Describe(err)
}

// liveness is how fast a dead connection is noticed. A stream that goes quiet
// (a NAT that forgot the flow, a laptop that slept) sends nothing, so without
// these only Listen's silence watchdog would catch it.
type liveness struct {
	// ping and pingTimeout: after ping of silence on an HTTP/2 connection the
	// transport sends a health-check ping and closes the connection if it is
	// not answered within pingTimeout.
	ping, pingTimeout time.Duration
	// tcpIdle, tcpInterval and tcpCount are the TCP keep-alive probes, which
	// cover plain HTTP/1.1 (a loopback hub) as well.
	tcpIdle, tcpInterval time.Duration
	tcpCount             int
}

// defaultLiveness detects a dead connection in about 8s over HTTP/2 and about
// 11s over TCP keep-alive.
var defaultLiveness = liveness{
	ping: 5 * time.Second, pingTimeout: 3 * time.Second,
	tcpIdle: 5 * time.Second, tcpInterval: 2 * time.Second, tcpCount: 3,
}

// newTransport is the standard transport with l's health checks.
func newTransport(l liveness) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.HTTP2 = &http.HTTP2Config{SendPingTimeout: l.ping, PingTimeout: l.pingTimeout}
	tr.DialContext = (&net.Dialer{
		Timeout:         30 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: l.tcpIdle, Interval: l.tcpInterval, Count: l.tcpCount},
	}).DialContext
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return tr
}
