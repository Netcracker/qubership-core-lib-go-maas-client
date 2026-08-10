package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
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
			// The M2M token is re-fetched on every attempt, so a 401 caused by
			// an expired token or a briefly unavailable token provider must be
			// retried rather than treated as a permanent auth failure.
			name: "expired M2M token: 401 twice then success",
			steps: []stepBehavior{
				{status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
				{status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
				{status: http.StatusOK, body: successBody},
			},
			expectSuccess:    true,
			expectedRequests: 3,
		},
		{
			name: "401 that never clears fails after the retry budget",
			steps: []stepBehavior{
				{status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
			},
			expectSuccess:    false,
			expectedRequests: 5,
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

// Test_Failover_RetryBudgetNotMultipliedByRestyRetry checks that retries
// aren't multiplied when the underlying HTTP client has its own retry
// configured on top of ours: with RetryAttempts=3, the server must see
// exactly 3 requests.
func Test_Failover_RetryBudgetNotMultipliedByRestyRetry(t *testing.T) {
	assertions := require.New(t)
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"error proxying request: maas-service unavailable"}`))
	}))
	defer ts.Close()

	httpClient := resty.New().SetRetryCount(10)

	client := &CrudClient{
		MaasAgentUrl:  ts.URL,
		Namespace:     testNamespace,
		HttpClient:    httpClient,
		Auth:          func(ctx context.Context) (string, error) { return testToken, nil },
		RetryAttempts: 3,
		RetryInterval: 5 * time.Millisecond,
	}

	_, err := client.GetOrCreateTopic(context.Background(), classifier.New("test"))
	assertions.Error(err)
	assertions.Equal(3, requestCount, "retries got multiplied instead of staying at the configured budget")
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
