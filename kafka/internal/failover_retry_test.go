package internal

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
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
// shape maas-agent can produce during a switchover.
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
			// the single case a 401 retry exists for
			name: "token expired in flight: 401 then success",
			steps: []stepBehavior{
				{status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
				{status: http.StatusOK, body: successBody},
			},
			expectSuccess:    true,
			expectedRequests: 2,
		},
		{
			// tighter than the generic budget: 1 + MaxAuthRetries, not RetryAttempts
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

// Test_Failover_TransportErrorSpendsExactlyTheConfiguredBudget pins the attempt
// count on the transport-error path - a maas-agent pod going away mid-request.
// Counting happens at the listener because a dropped connection never reaches a
// handler.
func Test_Failover_TransportErrorSpendsExactlyTheConfiguredBudget(t *testing.T) {
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
			// away mid-request, and a transport error for the HTTP client.
			_ = conn.Close()
		}
	}()

	client := &CrudClient{
		MaasAgentUrl:  agentUrl,
		Namespace:     testNamespace,
		HttpClient:    resty.New(),
		Auth:          func(ctx context.Context) (string, error) { return testToken, nil },
		RetryAttempts: 3,
		RetryInterval: 5 * time.Millisecond,
	}

	_, err = client.GetOrCreateTopic(context.Background(), classifier.New("test"))
	assertions.Error(err)

	_ = listener.Close()
	<-done

	assertions.Equal(int32(3), atomic.LoadInt32(&connections),
		"the transport-error path must spend exactly RetryAttempts attempts, no more and no fewer")
}

// Test_Failover_UnresponsiveAgentIsBoundedWithoutCallerDeadline checks that a
// call with context.Background() still returns: the agent keeps the connection
// open and never answers, so only the per-attempt timeout can end it.
func Test_Failover_UnresponsiveAgentIsBoundedWithoutCallerDeadline(t *testing.T) {
	assertions := require.New(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assertions.NoError(err)

	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, aErr := listener.Accept()
			if aErr != nil {
				return
			}
			mu.Lock()
			held = append(held, conn) // accepted and never answered
			mu.Unlock()
		}
	}()
	// one defer, so the listener is closed - releasing the accept loop - before
	// the accepted connections are
	defer func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			_ = conn.Close()
		}
	}()

	client := &CrudClient{
		MaasAgentUrl:   "http://" + listener.Addr().String(),
		Namespace:      testNamespace,
		HttpClient:     resty.New(),
		Auth:           func(ctx context.Context) (string, error) { return testToken, nil },
		RetryAttempts:  2,
		RetryInterval:  5 * time.Millisecond,
		AttemptTimeout: 100 * time.Millisecond,
	}

	start := time.Now()
	_, err = client.GetOrCreateTopic(context.Background(), classifier.New("test"))
	elapsed := time.Since(start)

	assertions.Error(err)
	assertions.Less(elapsed, 2*time.Second,
		"a caller without a deadline must still be bounded by AttemptTimeout, took %s", elapsed)
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
