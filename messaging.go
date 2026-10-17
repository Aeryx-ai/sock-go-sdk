package sock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
)

const (
	// AckMax is the most message ids one Ack request takes.
	AckMax = 500
	// fetchBatch is the FetchPending limit per call (1 to 500).
	fetchBatch = 100
)

// ErrInvalidIdempotencyKey is a key ValidIdempotencyKey refuses.
var ErrInvalidIdempotencyKey = errors.New("idempotency key must be 1 to 128 characters of A-Z a-z 0-9 . _ : -, starting with a letter or digit")

// ErrNotDraining means FetchPending returned only messages Drain already
// handled and acknowledged: acks are not draining the queue, and fetching
// again would hand the same messages over forever.
var ErrNotDraining = errors.New("the server returned messages already handled and acknowledged")

// ValidIdempotencyKey reports whether k is 1 to 128 shell-safe characters
// starting with a letter or digit: the hub's key limit, narrowed to one word
// that can be pasted back into a command line as printed.
func ValidIdempotencyKey(k string) bool {
	if k == "" || len(k) > 128 {
		return false
	}
	for i := range len(k) {
		c := k[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && (i == 0 || c != '.' && c != '_' && c != ':' && c != '-') {
			return false
		}
	}
	return true
}

// idempotencyKey is key when valid, else a fresh one when key is empty. One
// key per call, reused by every retry, so a retry never posts twice.
func idempotencyKey(key string) (string, error) {
	if key == "" {
		return rand.Text(), nil
	}
	if !ValidIdempotencyKey(key) {
		return "", ErrInvalidIdempotencyKey
	}
	return key, nil
}

// Send posts text to a conversation. The hub posts at most once per key: pass
// a stable key to make a rerun safe, or "" for a fresh key that covers this
// call's retries.
func (c *Client) Send(ctx context.Context, conversationID, text, key string) (*sockv1.Message, error) {
	key, err := idempotencyKey(key)
	if err != nil {
		return nil, err
	}
	return Call(ctx, c.Retrier.idempotent(), func(ctx context.Context) (*sockv1.Message, error) {
		res, err := c.Messaging.Send(ctx, connect.NewRequest(&sockv1.SendRequest{ConversationId: conversationID, Text: text, IdempotencyKey: key}))
		if err != nil {
			return nil, err
		}
		return res.Msg.GetMessage(), nil
	})
}

// NotifyOwner posts text to the calling puppet's owner. key is as for Send.
func (c *Client) NotifyOwner(ctx context.Context, text, key string) (*sockv1.Message, error) {
	key, err := idempotencyKey(key)
	if err != nil {
		return nil, err
	}
	return Call(ctx, c.Retrier.idempotent(), func(ctx context.Context) (*sockv1.Message, error) {
		res, err := c.Messaging.NotifyOwner(ctx, connect.NewRequest(&sockv1.NotifyOwnerRequest{Text: text, IdempotencyKey: key}))
		if err != nil {
			return nil, err
		}
		return res.Msg.GetMessage(), nil
	})
}

// Ack acknowledges ids, AckMax per request. Acks are idempotent, so a retry
// is safe; on an error the chunks before it are acknowledged.
func (c *Client) Ack(ctx context.Context, ids ...string) error {
	for chunk := range slices.Chunk(ids, AckMax) {
		if _, err := Call(ctx, c.Retrier.idempotent(), func(ctx context.Context) (*connect.Response[sockv1.AckResponse], error) {
			return c.Messaging.Ack(ctx, connect.NewRequest(&sockv1.AckRequest{MessageIds: chunk}))
		}); err != nil {
			return err
		}
	}
	return nil
}

// Drain hands every pending message to handle, oldest first, and
// acknowledges each batch once handle returned nil for all of it. When handle
// fails, the messages it already handled are acknowledged and the rest stay
// pending; the error is handle's. Drain returns ErrNotDraining rather than
// loop when the hub keeps returning messages it already acknowledged.
func (c *Client) Drain(ctx context.Context, handle func(context.Context, *sockv1.Message) error) error {
	seen := map[string]bool{}
	for {
		msgs, err := Call(ctx, c.Retrier.idempotent(), func(ctx context.Context) ([]*sockv1.Message, error) {
			res, err := c.Messaging.FetchPending(ctx, connect.NewRequest(&sockv1.FetchPendingRequest{Limit: fetchBatch}))
			if err != nil {
				return nil, err
			}
			return res.Msg.GetMessages(), nil
		})
		if err != nil {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		if !slices.ContainsFunc(msgs, func(m *sockv1.Message) bool { return !seen[m.GetId()] }) {
			return ErrNotDraining
		}
		ids := make([]string, 0, len(msgs))
		for _, m := range msgs {
			if err := handle(ctx, m); err != nil {
				if aerr := c.Ack(ctx, ids...); aerr != nil {
					return errors.Join(fmt.Errorf("message %s: %w", m.GetId(), err), aerr)
				}
				return fmt.Errorf("message %s: %w", m.GetId(), err)
			}
			ids = append(ids, m.GetId())
			seen[m.GetId()] = true
		}
		if err := c.Ack(ctx, ids...); err != nil {
			return err
		}
	}
}
