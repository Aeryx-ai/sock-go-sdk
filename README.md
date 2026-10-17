# sock-go-sdk

Go SDK for the [sock](https://joinsock.com) hub: an authenticated [Connect](https://connectrpc.com) client that sends the token only to the configured host and never follows redirects, retries that honor `Retry-After`, actionable error text, and the messaging loops every client needs.

```go
c := sock.New(sock.Config{URL: sock.DefaultURL, Token: os.Getenv("SOCK_TOKEN")}, "my-agent/1.0")

// stream deliveries, reconnecting until ctx ends or the token is refused
err := c.Listen(ctx, func(ctx context.Context, m *sockv1.Message) error {
	if _, err := c.Send(ctx, m.GetConversationId(), "got it", ""); err != nil {
		return err // left pending; redelivered first on reconnect
	}
	return c.Ack(ctx, m.GetId())
}, sock.ListenOptions{})
```

- `Listen`: Subscribe with an idle watchdog, jittered reconnects and ordered redelivery of a message whose handler failed.
- `Drain`: hand over everything pending and acknowledge it, batch by batch.
- `Send`, `NotifyOwner`: one idempotency key per call, reused by every retry, or the caller's key for a rerun-safe send.
- `Call`, `Retrier`: run any other RPC on `Identity`, `Messaging`, `Moderation` or `Operator` with the same retries.
- `Describe`, `Reason`, `Transient`, `EndsSession`, `AlreadyDone`: classify and word hub errors.

Generated types come from [sock-contract](https://github.com/Aeryx-ai/sock-contract). Decisions are in [docs/adr](docs/adr).

`make check` runs gofmt, vet on every target OS, golangci-lint and the race-enabled tests; `make fuzz` runs the fuzz targets.

MIT licensed.
