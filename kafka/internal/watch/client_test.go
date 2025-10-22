package watch

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testResource struct {
	classifier classifier.Keys
}

func (t testResource) GetClassifier() classifier.Keys {
	return t.classifier
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (rt roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt(req)
}

type mockHTTPClient struct {
	responses    []*resty.Response
	currentIndex int
	mu           sync.Mutex
}

// newMockHTTPClient creates a resty.Client with a custom HTTP transport.
func newMockHTTPClient(responses ...*resty.Response) *resty.Client {
	mock := &mockHTTPClient{
		responses: responses,
	}
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mock.mu.Lock()
			defer mock.mu.Unlock()
			if mock.currentIndex < len(mock.responses) {
				resp := mock.responses[mock.currentIndex]
				mock.currentIndex++
				return resp.RawResponse, nil
			}
			return nil, context.DeadlineExceeded
		}),
	}
	client := resty.NewWithClient(httpClient)
	return client
}

func TestWatchOnCreateResources_Success(t *testing.T) {
	// Setup
	cls := classifier.New("test").WithNamespace("ns1")
	resource := testResource{classifier: cls}

	mockResp := &resty.Response{
		RawResponse: &http.Response{
			StatusCode: 200,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body:       http.NoBody,
		},
	}

	client := NewClient[testResource]("http://test", "/watch", newMockHTTPClient(mockResp),
		func(response *resty.Response) ([]testResource, error) {
			return []testResource{resource}, nil
		})

	// Test
	called := make(chan testResource, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.WatchOnCreateResources(ctx, cls, func(r testResource) {
		called <- r
	})

	assert.NoError(t, err)

	select {
	case got := <-called:
		assert.Equal(t, resource, got)
	case <-ctx.Done():
		t.Fatal("timeout waiting for callback")
	}
}

func TestWatchOnCreateResources_MultipleWatchers(t *testing.T) {
	// Setup
	cls1 := classifier.New("test1").WithNamespace("ns1")
	cls2 := classifier.New("test2").WithNamespace("ns1")
	resource1 := testResource{classifier: cls1}
	resource2 := testResource{classifier: cls2}

	mockResp := &resty.Response{
		RawResponse: &http.Response{
			StatusCode: 200,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
		},
	}

	client := &DefaultClient[testResource]{
		watchUrl:   "http://test/watch",
		httpClient: newMockHTTPClient(mockResp),
		converter: func(response *resty.Response) ([]testResource, error) {
			return []testResource{resource1, resource2}, nil
		},
		watchLock: &sync.RWMutex{},
	}

	// Test
	called1 := make(chan testResource, 1)
	called2 := make(chan testResource, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err1 := client.WatchOnCreateResources(ctx, cls1, func(r testResource) {
		called1 <- r
	})
	err2 := client.WatchOnCreateResources(ctx, cls2, func(r testResource) {
		called2 <- r
	})

	assert.NoError(t, err1)
	assert.NoError(t, err2)

	select {
	case got1 := <-called1:
		assert.Equal(t, resource1, got1)
	case <-ctx.Done():
		t.Fatal("timeout waiting for callback 1")
	}

	select {
	case got2 := <-called2:
		assert.Equal(t, resource2, got2)
	case <-ctx.Done():
		t.Fatal("timeout waiting for callback 2")
	}
}

func TestRemove(t *testing.T) {
	items := []int{1, 2, 3, 4, 5}

	// Test removing existing item
	result := remove(items, 3)
	assert.Equal(t, []int{1, 2, 4, 5}, result)

	// Test removing non-existing item
	result = remove(items, 6)
	assert.Equal(t, items, result)

	// Test removing first item
	result = remove(items, 1)
	assert.Equal(t, []int{2, 3, 4, 5}, result)

	// Test removing last item
	result = remove(items, 5)
	assert.Equal(t, []int{1, 2, 3, 4}, result)
}

func TestWatchOnCreateResources_CancellationAndCleanup(t *testing.T) {
	cls := classifier.New("test").WithNamespace("ns1")
	// Create an HTTP client that always returns context.Canceled.
	httpClient := resty.NewWithClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, context.Canceled
		}),
	})

	client := &DefaultClient[testResource]{
		watchUrl:   "http://test/watch",
		httpClient: httpClient,
		converter: func(response *resty.Response) ([]testResource, error) {
			return nil, nil
		},
		watchLock: &sync.RWMutex{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	err := client.WatchOnCreateResources(ctx, cls, func(r testResource) {})
	assert.NoError(t, err)

	// Cancel the context so that the watch loop takes the cancellation branch.
	cancel()
	// Wait until watchCancel is cleared
	assert.Eventually(t, func() bool {
		client.watchLock.RLock()
		defer client.watchLock.RUnlock()
		return client.watchCancel == nil
	}, 500*time.Millisecond, 50*time.Millisecond, "watchCancel should be cleared after cancellation")
}

func TestWatch_InvalidClassifier_MissingFields(t *testing.T) {
	b := NewTenantWatchClient[mockResource](
		"http://example.com",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]mockResource, error) {
			return nil, nil

		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)

	err := b.Watch(context.Background(), classifier.Keys{}, func([]mockResource, error) {})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "classifier must contain both name and namespace")
}

func TestWatch_InvalidClassifier_WithTenantId(t *testing.T) {
	b := NewTenantWatchClient[mockResource](
		"http://example.com",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]mockResource, error) {
			return nil, nil
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)

	classifier := classifier.Keys{
		classifier.Name:      "res1",
		classifier.Namespace: "ns",
		classifier.TenantId:  "some-tenant",
	}
	err := b.Watch(context.Background(), classifier, func([]mockResource, error) {})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "classifier cannot contain")
}

func TestWatch_ValidClassifier_StartsBroadcasterAndTriggersCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var called bool
	var received []testResource

	client := NewTenantWatchClient[testResource](
		"http://example.com",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			return []testResource{{classifier: keys}}, nil
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)

	client.connectToWebSocket = func(ctx context.Context, tenantManagerUrl string, dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error), onConnect func()) error {
		onConnect()
		return nil
	}

	callback := func(r []testResource, err error) {
		mu.Lock()
		defer mu.Unlock()
		called = true
		received = r
	}

	// Watch should not error on valid input
	err := client.Watch(ctx, classifier.Keys{
		classifier.Name:      "r1",
		classifier.Namespace: "ns",
	}, callback)
	require.NoError(t, err)

	// Simulate broadcaster notifying tenants manually
	client.NotifyForTest([]watch.Tenant{{ExternalId: "t1", Status: "active"}})

	timeout := time.After(500 * time.Millisecond)
	tick := time.Tick(5 * time.Millisecond)

	waited := false
	for !waited {
		select {
		case <-timeout:
			waited = true
		case <-tick:
			if called {
				waited = true
				break
			}
		}
	}

	assert.True(t, called, "callback should be triggered")
	require.Len(t, received, 1)
	assert.Equal(t, "r1", received[0].classifier["name"])
	assert.Equal(t, "ns", received[0].classifier["namespace"])
}

func (b *TenantWatchBroadcaster[T]) NotifyForTest(tenants []watch.Tenant) {
	if b.internalProcCtx == nil {
		b.internalProcCtx, b.cancel = context.WithCancel(context.Background())
		go b.processLoop(b.internalProcCtx)
	}
	select {
	case b.tenants <- tenants:
	default:
	}
}
