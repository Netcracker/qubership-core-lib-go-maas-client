// Package rest carries the CRUD call shape shared by the Kafka and Rabbit
// clients: one retry limit, one per-attempt timeout, one error classification.
package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
)

// Caller runs CRUD calls to maas-agent.
type Caller struct {
	HttpClient *resty.Client
	Logger     logging.Logger
	// MaxTotalDuration bounds a whole call, retries included.
	MaxTotalDuration time.Duration
	// AttemptTimeout bounds one request.
	AttemptTimeout time.Duration
}

// Send issues send until it succeeds or the total duration runs out. Statuses in
// emptyOn end the call successfully with a nil response.
func (c Caller) Send(ctx context.Context, send func(*resty.Request) (*resty.Response, error),
	emptyOn ...int) (*resty.Response, error) {
	classifier := util.NewResponseClassifier()
	var result *resty.Response
	err := util.NewRetryWithin(c.MaxTotalDuration).RunCtx(ctx, func(ctx context.Context) error {
		ctx, cancel := util.AttemptContext(ctx, c.AttemptTimeout)
		defer cancel()

		result = nil
		response, err := send(c.HttpClient.R().SetContext(ctx))
		if err != nil {
			return fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
		}
		c.Logger.InfoC(ctx, "Received response: %d", response.StatusCode())
		if slices.Contains(emptyOn, response.StatusCode()) {
			return nil
		}
		if !response.IsSuccess() {
			return classifier.Classify(response.StatusCode(), response.Status(), response.String())
		}
		result = response
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Decode parses a response body. A nil response, as returned for a status in
// emptyOn, decodes to nil.
func Decode[T any](response *resty.Response) (*T, error) {
	if response == nil {
		return nil, nil
	}
	var value T
	if err := json.Unmarshal(response.Body(), &value); err != nil {
		return nil, fmt.Errorf("failed to parse response from maas-agent. Cause: %w", err)
	}
	return &value, nil
}
