package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/stretchr/testify/require"
)

// failoverRabbitClient builds a client with a total duration short enough for a test.
func failoverRabbitClient(agentUrl string) *MaasClient {
	return NewRabbitClient(testNamespace, &CrudClient{
		MaasAgentUrl:     agentUrl,
		Namespace:        testNamespace,
		HttpClient:       resty.New(),
		MaxTotalDuration: time.Second,
	})
}

// Test_Failover_GetOrCreateVhost_RetriesOn500 checks that a transient 500 is
// retried and eventually succeeds.
func Test_Failover_GetOrCreateVhost_RetriesOn500(t *testing.T) {
	assertions := require.New(t)
	// atomic: each request is served on its own goroutine
	var requestCount int64
	successBody := `{"cnn":"amqp://127.0.0.1:5672/namespace.test","username":"user","password":"plain:password"}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt64(&requestCount, 1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"error proxying request: maas-service unavailable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(successBody))
	}))
	defer ts.Close()

	rabbitClient := failoverRabbitClient(ts.URL)

	vhost, err := rabbitClient.GetOrCreateVhost(context.Background(), classifier.New("test"))
	assertions.NoError(err)
	assertions.NotNil(vhost)
	assertions.Equal(int64(3), atomic.LoadInt64(&requestCount),
		"transient 5xx should be retried")
}

// Test_Failover_GetOrCreateVhost_400NotRetried checks that a plain 4xx fails
// on the first attempt.
func Test_Failover_GetOrCreateVhost_400NotRetried(t *testing.T) {
	assertions := require.New(t)
	var requestCount int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer ts.Close()

	rabbitClient := failoverRabbitClient(ts.URL)

	_, err := rabbitClient.GetOrCreateVhost(context.Background(), classifier.New("test"))
	assertions.Error(err)
	assertions.Equal(int64(1), atomic.LoadInt64(&requestCount), "400 must fail immediately, not be retried")
}

// Test_Failover_GetOrCreateVhost_UnparseableBodyIsPermanent checks that a 200 the
// client cannot parse fails on the spot: the same server answers the same way, so
// repeating it only delays the error.
func Test_Failover_GetOrCreateVhost_UnparseableBodyIsPermanent(t *testing.T) {
	assertions := require.New(t)
	var requestCount int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"username":123}`)) // username is a string in the model
	}))
	defer ts.Close()

	rabbitClient := failoverRabbitClient(ts.URL)

	_, err := rabbitClient.GetOrCreateVhost(context.Background(), classifier.New("test"))
	assertions.ErrorContains(err, "failed to parse response")
	assertions.Equal(int64(1), atomic.LoadInt64(&requestCount), "a parse failure must not be retried")
}
