package sock

import (
	"errors"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

func withReason(ce *connect.Error, reason sockv1.ErrorReason) *connect.Error {
	d, err := connect.NewErrorDetail(&sockv1.ErrorInfo{Reason: reason})
	if err != nil {
		panic(err)
	}
	ce.AddDetail(d)
	return ce
}

func RateLimited(retryAfter string) error {
	ce := withReason(connect.NewError(connect.CodeResourceExhausted, errors.New("rate limited")), sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED)
	ce.Meta().Set("Retry-After", retryAfter)
	return ce
}

func Unavailable() error {
	return connect.NewError(connect.CodeUnavailable, errors.New("database unreachable"))
}

func NotMember() error {
	return withReason(connect.NewError(connect.CodePermissionDenied, errors.New("not a member")), sockv1.ErrorReason_ERROR_REASON_NOT_A_MEMBER)
}
