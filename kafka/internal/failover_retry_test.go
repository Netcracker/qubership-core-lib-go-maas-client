package internal

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/stretchr/testify/require"
)

// stepBehavior describes one stub response. status == 0 resets the
// connection instead of responding.
type stepBehavior struct {
	status int
	body   string
}

// newSequencedServer plays back steps in order for requests to path,
// repeating the last one once exhausted. *requestCount counts matching
// requests.
func newSequencedServer(path string, steps []stepBehavior, requestCount *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		idx := *requestCount
		*requestCount++
		var step stepBehavior
		if idx < len(steps) {
			step = steps[idx]
		} else {
			step = steps[len(steps)-1]
		}
		if step.status == 0 {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
					return
				}
			}
			return
		}
		w.WriteHeader(step.status)
		if step.body != "" {
			_, _ = w.Write([]byte(step.body))
		}
	}))
}

func failoverCrudClient(agentUrl string, retryAttempts int, retryInterval time.Duration) *CrudClient {
	return &CrudClient{
		MaasAgentUrl:  agentUrl,
		Namespace:     testNamespace,
		HttpClient:    resty.New(),
		Auth:          func(ctx context.Context) (string, error) { return testToken, nil },
		RetryAttempts: retryAttempts,
		RetryInterval: retryInterval,
	}
}

// Test_Failover_ResponseTable checks GetOrCreateTopic against each response
// shape maas-agent can produce: read-only (405), agent-down (500), expired
// token (401) and connection resets must be retried and eventually succeed,
// while genuinely permanent codes (400, 403) must fail on the first attempt.
func Test_Failover_ResponseTable(t *testing.T) {
	successBody := `{"addresses":{"PLAINTEXT":["b1:9092"]},"name":"maas.test-namespace.test.topic","classifier":{"name":"test.topic","namespace":"test-namespace"}}`
	tmf405Body := `{"code":"MAAS-0600","reason":"database is in read-only mode"}`
	agentDown500Body := `{"error":"error proxying request: dial tcp: connect: connection refused"}`

	cases := []struct {
		name             string
		steps            []stepBehavior
		expectSuccess    bool
		expectedRequests int
	}{
		{
			name: "read-only Postgres: 405 TMF twice then success",
			steps: []stepBehavior{
				{status: http.StatusMethodNotAllowed, body: tmf405Body},
				{status: http.StatusMethodNotAllowed, body: tmf405Body},
				{status: http.StatusOK, body: successBody},
			},
			expectSuccess:    true,
			expectedRequests: 3,
		},
		{
			name: "maas-service unreachable: agent's own 500 twice then success",
			steps: []stepBehavior{
				{status: http.StatusInternalServerError, body: agentDown500Body},
				{status: http.StatusInternalServerError, body: agentDown500Body},
				{status: http.StatusOK, body: successBody},
			},
			expectSuccess:    true,
			expectedRequests: 3,
		},
		{
			name: "agent restarting: connection reset then success",
			steps: []stepBehavior{
				{status: 0},
				{status: http.StatusOK, body: successBody},
			},
			expectSuccess:    true,
			expectedRequests: 2,
		},
		{
			name: "400 fails immediately, is not retried",
			steps: []stepBehavior{
				{status: http.StatusBadRequest, body: `{"error":"bad request"}`},
			},
			expectSuccess:    false,
			expectedRequests: 1,
		},
		{
			// The M2M token is re-fetched on every attempt, so a token that expired
			// in flight clears on the next one - the single case a 401 retry exists for.
			name: "token expired in flight: 401 then success",
			steps: []stepBehavior{
				{status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
				{status: http.StatusOK, body: successBody},
			},
			expectSuccess:    true,
			expectedRequests: 2,
		},
		{
			// A 401 that keeps coming back means the provider is handing out a token
			// the server rejects, and it has no way of being told so. Further attempts
			// resend the same token, so the budget is deliberately tighter than the
			// generic one: 1 + MaxAuthRetries, not the full RetryAttempts of 5.
			name: "rejected credentials: 401 gives up after MaxAuthRetries, not the full budget",
			steps: []stepBehavior{
				{status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
			},
			expectSuccess:    false,
			expectedRequests: util.MaxAuthRetries + 1,
		},
		{
			name: "403 fails immediately, is not retried",
			steps: []stepBehavior{
				{status: http.StatusForbidden, body: `{"error":"forbidden"}`},
			},
			expectSuccess:    false,
			expectedRequests: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertions := require.New(t)
			requestCount := 0
			ts := newSequencedServer("/api/v1/kafka/topic", tc.steps, &requestCount)
			defer ts.Close()

			client := failoverCrudClient(ts.URL, 5, 5*time.Millisecond)

			_, err := client.GetOrCreateTopic(context.Background(), classifier.New("test"))
			if tc.expectSuccess {
				assertions.NoError(err)
			} else {
				assertions.Error(err)
			}
			assertions.Equal(tc.expectedRequests, requestCount, "unexpected number of requests reaching maas-agent")
		})
	}
}

// Test_Failover_GetTopic_404NotRetried checks that a 404 returns (nil, nil)
// after exactly one request.
func Test_Failover_GetTopic_404NotRetried(t *testing.T) {
	assertions := require.New(t)
	requestCount := 0
	ts := newSequencedServer("/api/v1/kafka/topic/get-by-classifier",
		[]stepBehavior{{status: http.StatusNotFound}}, &requestCount)
	defer ts.Close()

	client := failoverCrudClient(ts.URL, 5, 5*time.Millisecond)

	topic, err := client.GetTopic(context.Background(), classifier.New("test"))
	assertions.NoError(err)
	assertions.Nil(topic)
	assertions.Equal(1, requestCount, "404 must not be retried")
}

// Test_Failover_RetryBudgetNotMultipliedByRestyRetry checks that our retry
// budget is not multiplied by a retry configured on the underlying HTTP client.
//
// The fault has to be a transport error, not a 5xx: resty retries when the
// round trip returns an error, and a 5xx response is a successful round trip, so
// it is left alone unless an explicit RetryCondition is registered. Testing this
// with a 5xx would assert something that holds regardless of SetRetryCount and
// would miss the case that actually multiplies - a rescheduled maas-agent
// refusing connections.
//
// Counting happens at the listener rather than in a handler, because a refused
// connection never reaches one.
func Test_Failover_RetryBudgetNotMultipliedByRestyRetry(t *testing.T) {
	assertions := require.New(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assertions.NoError(err)
	agentUrl := "http://" + listener.Addr().String()

	var connections int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, aErr := listener.Accept()
			if aErr != nil {
				return
			}
			atomic.AddInt32(&connections, 1)
			// Close without answering: the same shape as an agent whose pod is going
			// away mid-request, and a transport error for resty.
			_ = conn.Close()
		}
	}()

	httpClient := resty.New().SetRetryCount(10)

	client := &CrudClient{
		MaasAgentUrl:  agentUrl,
		Namespace:     testNamespace,
		HttpClient:    httpClient,
		Auth:          func(ctx context.Context) (string, error) { return testToken, nil },
		RetryAttempts: 3,
		RetryInterval: 5 * time.Millisecond,
	}

	_, err = client.GetOrCreateTopic(context.Background(), classifier.New("test"))
	assertions.Error(err)

	_ = listener.Close()
	<-done

	assertions.Equal(int32(3), atomic.LoadInt32(&connections),
		"resty retried on top of our budget: expected 3 attempts, the client made more")
}

// Test_Failover_ContextCancellation checks that a short context deadline
// aborts retries promptly instead of waiting out the full retry budget.
func Test_Failover_ContextCancellation(t *testing.T) {
	assertions := require.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	client := failoverCrudClient(ts.URL, 5, 200*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.GetOrCreateTopic(ctx, classifier.New("test"))
	elapsed := time.Since(start)

	assertions.Error(err)
	assertions.Less(elapsed, 400*time.Millisecond, "should abort once context is done instead of exhausting all retries")
}
