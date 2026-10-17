package sock_test

import (
	"context"
	"log"
	"os"

	sock "github.com/aeryx-ai/sock-go-sdk"
	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
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
