package watch

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func errorResponse(code int) *resty.Response {
	return &resty.Response{RawResponse: &http.Response{
		StatusCode: code,
		Proto:      "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: make(http.Header),
		Body:   http.NoBody,
	}}
}

// countingWatchClient builds a DefaultClient whose watch requests always fail
// with 503, counting the attempts. Intervals are applied as-is, so zero values
// exercise the fallback path.
func countingWatchClient(count *int32, retryInterval, maxRetryInterval time.Duration) *DefaultClient[testResource] {
	httpClient := resty.NewWithClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(count, 1)
			return errorResponse(503).RawResponse, nil
		}),
	})
	return &DefaultClient[testResource]{
		watchUrl:   "http://test/watch",
		httpClient: httpClient,
		converter: func(response *resty.Response) ([]testResource, error) {
			return nil, nil
		},
		watchLock:        &sync.RWMutex{},
		RetryInterval:    retryInterval,
		MaxRetryInterval: maxRetryInterval,
	}
}

// runFailingWatchFor starts the watch, lets it run for window, then cancels and
// reports how many requests reached the stub.
func runFailingWatchFor(t *testing.T, client *DefaultClient[testResource], count *int32, window time.Duration) int {
	t.Helper()
	cls := classifier.New("test").WithNamespace("ns1")

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.WatchOnCreateResources(ctx, cls, func(r testResource) {}))

	time.Sleep(window)
	cancel()

	return int(atomic.LoadInt32(count))
}

func TestWatchOnCreateResources_BusyLoopIsBounded(t *testing.T) {
	var count int32
	// 100ms backoff over a 300ms window: with backoff ~3 attempts, without it thousands
	attempts := runFailingWatchFor(t, countingWatchClient(&count, 100*time.Millisecond, 300*time.Millisecond),
		&count, 300*time.Millisecond)

	// the lower bound keeps the test from passing when the loop never ran
	assert.GreaterOrEqual(t, attempts, 1, "watch loop did not run at all — the test would pass vacuously")
	assert.LessOrEqual(t, attempts, 10, "no backoff between failed watch requests")
}

// TestWatchOnCreateResources_BusyLoopIsBoundedWithoutExplicitIntervals checks
// that a client built via a struct literal that skips RetryInterval and
// MaxRetryInterval (bypassing NewClient) still backs off between failures
// instead of busy-looping with a zero delay.
func TestWatchOnCreateResources_BusyLoopIsBoundedWithoutExplicitIntervals(t *testing.T) {
	var count int32
	// zero intervals must fall back to util.DefaultRetryInterval, not to no delay
	attempts := runFailingWatchFor(t, countingWatchClient(&count, 0, 0), &count, 300*time.Millisecond)

	assert.GreaterOrEqual(t, attempts, 1, "watch loop did not run at all — the test would pass vacuously")
	assert.LessOrEqual(t, attempts, 3, "zero-valued intervals fell through to a busy loop")
}

func TestWatchOnCreateResources_RecoversAfterTransientErrors(t *testing.T) {
	cls := classifier.New("test").WithNamespace("ns1")
	resource := testResource{classifier: cls}

	successResponse := &resty.Response{RawResponse: &http.Response{
		StatusCode: 200,
		Proto:      "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: make(http.Header),
		Body:   http.NoBody,
	}}

	client := NewClient[testResource]("http://test", "/watch",
		newMockHTTPClient(errorResponse(503), errorResponse(503), successResponse),
		func(response *resty.Response) ([]testResource, error) {
			return []testResource{resource}, nil
		})

	called := make(chan testResource, 1)
	// two 503s cost ~1s + ~2s of backoff at the default interval, so a 5s deadline
	// leaves almost no slack on a loaded runner
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := client.WatchOnCreateResources(ctx, cls, func(r testResource) {
		called <- r
	})
	require.NoError(t, err)

	select {
	case got := <-called:
		assert.Equal(t, resource, got)
	case <-ctx.Done():
		t.Fatal("timeout waiting for callback after transient errors")
	}
}
