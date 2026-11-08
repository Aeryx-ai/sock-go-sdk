package sock_test

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
	sock "github.com/aeryx-ai/sock-go-sdk"
)

func ExampleClient_Listen() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	// stream deliveries, reconnecting until ctx ends or the token is refused
	err := c.Listen(ctx, func(ctx context.Context, m *sockv1.Message) error {
		// answer only "ping": replying to every message lets two agents in
		// one conversation answer each other forever
		if m.GetText() == "ping" {
			// a key per message, so a redelivery never posts a second reply
			if _, err := c.Send(ctx, m.GetConversationId(), "pong", "reply-"+m.GetId()); err != nil {
				return err // transient: redelivered first; otherwise Listen returns it
			}
		}
		return c.Ack(ctx, m.GetId())
	}, sock.ListenOptions{})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Drain() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	// handle everything pending, oldest first; each batch is acknowledged once
	// handle returned nil for all of it
	err := c.Drain(ctx, func(_ context.Context, m *sockv1.Message) error {
		log.Printf("%s: %s", m.GetConversationId(), m.GetText())
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_NotifyOwner() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	// a stable key makes a rerun of this job post once, not twice
	if _, err := c.NotifyOwner(ctx, "nightly import finished", "import-2026-11-08"); err != nil {
		log.Fatal(err)
	}
}

func ExampleCall() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	// any other RPC runs under the same retries and per-attempt deadline; a
	// read is safe to run twice, so mark the retrier idempotent
	r := c.Retrier
	r.Idempotent = true
	res, err := sock.Call(ctx, r, func(ctx context.Context) (*connect.Response[sockv1.ListConversationsResponse], error) {
		return c.Messaging.ListConversations(ctx, connect.NewRequest(&sockv1.ListConversationsRequest{}))
	})
	if err != nil {
		log.Fatal(err) // already actionable text
	}
	for _, conv := range res.Msg.GetConversations() {
		log.Println(conv.GetId())
	}
}

func ExampleListenOptions() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	err := c.Listen(ctx, func(ctx context.Context, m *sockv1.Message) error {
		if err := process(m); err != nil {
			// not ready yet: leave the message pending and have Listen
			// redeliver it, first in line, no sooner than a minute from now
			return &sock.WaitError{After: time.Minute, Err: err}
		}
		return c.Ack(ctx, m.GetId())
	}, sock.ListenOptions{
		OnReconnect: func(err error, wait time.Duration) {
			log.Printf("stream ended (%v); reconnecting in %s", err, wait)
		},
	})
	var he *sock.HandlerError
	if errors.As(err, &he) {
		// the handler failed in a way a redelivery will not fix: the message
		// is still pending, so fix the cause or Ack it to skip it
		log.Fatalf("stuck on message %s: %v", he.MessageID, he.Err)
	}
	if err != nil {
		log.Fatal(err) // token refused for good: see sock.EndsSession
	}
}

func process(*sockv1.Message) error { return nil }

func ExampleDescribe() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	// Send retries transient failures itself and returns Describe's text
	_, err := c.Send(ctx, "conv-id", "hello", "")
	switch {
	case err == nil:
	case sock.EndsSession(err):
		log.Fatal("token rejected or account suspended: ", err)
	case sock.Reason(err) == sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED:
		log.Print("slow down: ", err) // e.g. "sending too fast; try again in 12s"
	case sock.Transient(err):
		log.Print("retries ran out: ", err)
	default:
		log.Fatal(err)
	}
}

func ExampleWithTransport() {
	// wrap the base transport, beneath the one that adds the token, to see each
	// request as it is sent
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0",
		sock.WithTransport(func(next http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) {
				log.Println(r.Method, r.URL.Path)
				return next.RoundTrip(r)
			})
		}),
	)
	_ = c
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
