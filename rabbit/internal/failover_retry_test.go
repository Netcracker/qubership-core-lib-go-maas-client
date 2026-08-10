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

// Test_Failover_GetOrCreateVhost_RetriesOn500 checks that a transient 500 is
// retried and eventually succeeds.
func Test_Failover_GetOrCreateVhost_RetriesOn500(t *testing.T) {
	assertions := require.New(t)
	requestCount := 0
	successBody := `{"cnn":"amqp://127.0.0.1:5672/namespace.test","username":"user","password":"plain:password"}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"error proxying request: maas-service unavailable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(successBody))
	}))
	defer ts.Close()

	client := &CrudClient{
		MaasAgentUrl:  ts.URL,
		Namespace:     testNamespace,
		HttpClient:    resty.New(),
		RetryAttempts: 5,
		RetryInterval: 5 * time.Millisecond,
	}
	rabbitClient := NewRabbitClient(testNamespace, client)

	vhost, err := rabbitClient.GetOrCreateVhost(context.Background(), classifier.New("test"))
	assertions.NoError(err)
	assertions.NotNil(vhost)
	assertions.Equal(3, requestCount,
		"transient 5xx should be retried")
}

// Test_Failover_GetOrCreateVhost_400NotRetried checks that a plain 4xx fails
// on the first attempt.
func Test_Failover_GetOrCreateVhost_400NotRetried(t *testing.T) {
	assertions := require.New(t)
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer ts.Close()

	client := &CrudClient{
		MaasAgentUrl:  ts.URL,
		Namespace:     testNamespace,
		HttpClient:    resty.New(),
		RetryAttempts: 5,
		RetryInterval: 5 * time.Millisecond,
	}
	rabbitClient := NewRabbitClient(testNamespace, client)

	_, err := rabbitClient.GetOrCreateVhost(context.Background(), classifier.New("test"))
	assertions.Error(err)
	assertions.Equal(1, requestCount, "400 must fail immediately, not be retried")
}

// Test_Failover_GetOrCreateVhost_StaleFieldDoesNotLeakAcrossRetries checks that
// a field set by a partially-parsed response on a failed attempt does not
// survive into the result of a later, successful attempt whose body omits
// that field.
func Test_Failover_GetOrCreateVhost_StaleFieldDoesNotLeakAcrossRetries(t *testing.T) {
	assertions := require.New(t)
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
		if requestCount == 1 {
			// "password" parses first and sticks before "username" (a number
			// instead of a string) aborts the unmarshal.
			_, _ = w.Write([]byte(`{"password":"leaked-old-password","username":123}`))
			return
		}
		// Second attempt succeeds and does not mention "password" at all.
		_, _ = w.Write([]byte(`{"cnn":"amqp://second","username":"user2"}`))
	}))
	defer ts.Close()

	client := &CrudClient{
		MaasAgentUrl:  ts.URL,
		Namespace:     testNamespace,
		HttpClient:    resty.New(),
		RetryAttempts: 5,
		RetryInterval: 5 * time.Millisecond,
	}
	rabbitClient := NewRabbitClient(testNamespace, client)

	vhost, err := rabbitClient.GetOrCreateVhost(context.Background(), classifier.New("test"))
	assertions.NoError(err)
	assertions.NotNil(vhost)
	assertions.Equal("amqp://second", vhost.Cnn)
	assertions.Equal("", vhost.EncodedPassword,
		"password from the failed first attempt must not leak into the successful result")
}
