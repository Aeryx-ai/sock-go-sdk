package sock_test

import (
	"context"
	"os"

	sock "github.com/aeryx-ai/sock-go-sdk"
	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

func ExampleClient_Listen() {
	ctx := context.Background()
	c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

	// stream deliveries, reconnecting until ctx ends or the token is refused
	_ = c.Listen(ctx, func(ctx context.Context, m *sockv1.Message) error {
		if _, err := c.Send(ctx, m.GetConversationId(), "got it", ""); err != nil {
			return err // left pending; redelivered first on reconnect
		}
		return c.Ack(ctx, m.GetId())
	}, sock.ListenOptions{})
}
