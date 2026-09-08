package watch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Once the connection retry attempts run out, the broadcaster gives up and
// surfaces the error; a later Watch call must revive it and retry the
// connection again rather than leaving it permanently dead.
func Test_TenantWatch_GivesUpAfterRetriesExhausted_ThenRevivesOnNextWatch(t *testing.T) {
	origAttempts := util.DefaultRetryAttempts
	origInterval := util.DefaultRetryInterval
	util.DefaultRetryAttempts = 2
	util.DefaultRetryInterval = 5 * time.Millisecond
	defer func() {
		util.DefaultRetryAttempts = origAttempts
		util.DefaultRetryInterval = origInterval
	}()

	var connectAttempts int32
	client := NewTenantWatchClient[testResource](
		"http://example.com",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			return nil, nil
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)
	client.connectToWebSocket = func(ctx context.Context, tenantManagerUrl string, dialer *websocket.Dialer,
		authSupplier func(ctx context.Context) (string, error), onConnect func()) error {
		atomic.AddInt32(&connectAttempts, 1)
		return errors.New("connection refused")
	}

	keys := classifier.Keys{classifier.Name: "r1", classifier.Namespace: "ns"}

	err := client.Watch(context.Background(), keys, func(resources []testResource, err error) {})
	require.Error(t, err)
	firstRoundAttempts := atomic.LoadInt32(&connectAttempts)
	assert.GreaterOrEqual(t, int(firstRoundAttempts), 3)

	err = client.Watch(context.Background(), keys, func(resources []testResource, err error) {})
	require.Error(t, err)
	assert.Greater(t, atomic.LoadInt32(&connectAttempts), firstRoundAttempts)
}

// notifyWatchers must not hold the broadcaster lock while fetching resources:
// the fetch is an HTTP call, and blocking on it there stops the websocket read
// loop, which needs the same lock to merge incoming events.
func Test_TenantWatch_SlowNotifyDoesNotBlockEventMerging(t *testing.T) {
	fetching := make(chan struct{})
	releaseFetch := make(chan struct{})

	b := &TenantWatchBroadcaster[testResource]{
		tenants: make(chan []watch.Tenant),
		getResources: func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			close(fetching)
			<-releaseFetch
			return nil, nil
		},
	}
	userCtx, cancelUser := context.WithCancel(context.Background())
	defer cancelUser()
	b.watchers = []*watcher[testResource]{{
		name:      "r1",
		namespace: "ns",
		userCtx:   userCtx,
		cancel:    cancelUser, // stop() derefs it if the fetch ever returns an error
		queue:     make(chan []testResource, 1),
	}}

	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		b.notifyWatchers(context.Background(), []watch.Tenant{{ExternalId: "1", Status: watch.StatusActive}})
	}()

	<-fetching // notifyWatchers is now inside the fetch

	merged := make(chan bool, 1)
	go func() {
		merged <- b.mergeTenants(&watch.TenantWatchEvent{
			Type:    watch.SUBSCRIBED,
			Tenants: []watch.Tenant{{ExternalId: "2", Status: watch.StatusActive}},
		})
	}()

	select {
	case <-merged:
	case <-time.After(2 * time.Second):
		t.Fatal("mergeTenants blocked while notifyWatchers was fetching: the lock is still held across the fetch")
	}

	close(releaseFetch)
	<-notifyDone
}

// The fetch is bound to the round context, so ending a round surfaces as a
// context error. That is not a failure of the watcher, and stopping it there
// would drop a healthy subscription.
func Test_TenantWatch_RoundCancellationDoesNotStopWatcher(t *testing.T) {
	fetching := make(chan struct{})
	b := &TenantWatchBroadcaster[testResource]{
		tenants: make(chan []watch.Tenant),
		getResources: func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			close(fetching)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	userCtx, cancelUser := context.WithCancel(context.Background())
	defer cancelUser()
	b.watchers = []*watcher[testResource]{{
		name:      "r1",
		namespace: "ns",
		userCtx:   userCtx,
		cancel:    cancelUser,
		queue:     make(chan []testResource, 1),
	}}

	roundCtx, cancelRound := context.WithCancel(context.Background())
	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		b.notifyWatchers(roundCtx, []watch.Tenant{{ExternalId: "1", Status: watch.StatusActive}})
	}()

	<-fetching // the fetch is in flight
	cancelRound()

	select {
	case <-notifyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("notifyWatchers did not return after its round was cancelled")
	}
	assert.NoError(t, userCtx.Err(), "cancelling a round must not stop the watcher")
}

// A watcher that stops draining its queue must not wedge the broadcaster: the
// send is bounded by the watcher's own context, which removeWatcher also waits
// on, so holding the lock across it would deadlock the two.
func Test_TenantWatch_UndrainedWatcherDoesNotWedgeNotify(t *testing.T) {
	b := &TenantWatchBroadcaster[testResource]{
		tenants: make(chan []watch.Tenant),
		getResources: func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			return []testResource{{}}, nil
		},
	}
	userCtx, cancelUser := context.WithCancel(context.Background())
	w := &watcher[testResource]{
		name:      "r1",
		namespace: "ns",
		userCtx:   userCtx,
		cancel:    cancelUser,
		queue:     make(chan []testResource, 1),
	}
	w.queue <- []testResource{{}} // queue already full, nobody is draining it
	b.watchers = []*watcher[testResource]{w}

	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		b.notifyWatchers(context.Background(), []watch.Tenant{{ExternalId: "1", Status: watch.StatusActive}})
	}()

	cancelUser() // the watcher goes away instead of draining

	select {
	case <-notifyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("notifyWatchers is stuck on a queue nobody drains")
	}
}

// A call that joins a round which then gives up has its registration dropped by
// the cleanup. It must re-register against the next round rather than return
// nil, which the caller would read as a live subscription.
func Test_TenantWatch_DroppedRegistrationReturnsError(t *testing.T) {
	origAttempts := util.DefaultRetryAttempts
	origInterval := util.DefaultRetryInterval
	util.DefaultRetryAttempts = 0 // a round gives up on its first failure
	util.DefaultRetryInterval = time.Millisecond
	defer func() {
		util.DefaultRetryAttempts = origAttempts
		util.DefaultRetryInterval = origInterval
	}()

	var client *TenantWatchBroadcaster[testResource]
	var connectCalls int32
	var secondJoined atomic.Bool
	client = NewTenantWatchClient[testResource](
		"http://example.com",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			return nil, nil
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)
	client.connectToWebSocket = func(ctx context.Context, tenantManagerUrl string, dialer *websocket.Dialer,
		authSupplier func(ctx context.Context) (string, error), onConnect func()) error {
		// hold the first round open until the second call has joined it, so that call
		// is inside startOnce.Do when the round is torn down
		if atomic.AddInt32(&connectCalls, 1) == 1 {
			secondJoined.Store(waitForWatchers(client, 2))
		}
		return errors.New("connection refused")
	}

	callback := func(resources []testResource, err error) {}
	first := make(chan error, 1)
	go func() {
		first <- client.Watch(context.Background(),
			classifier.Keys{classifier.Name: "r1", classifier.Namespace: "ns"}, callback)
	}()

	second := client.Watch(context.Background(),
		classifier.Keys{classifier.Name: "r2", classifier.Namespace: "ns"}, callback)

	require.True(t, secondJoined.Load(), "the second call did not join the first round, the window was not exercised")
	require.Error(t, second, "a call whose registration was dropped must not report success")
	require.Error(t, <-first)
	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&connectCalls)), 2, "the dropped call must open a round of its own")
}

// A Watch that never came up reports through the callback as well as through
// the returned error. Callers drive their reconnect loop from the callback, so
// dropping it there leaves them waiting.
func Test_TenantWatch_FailedStartReachesTheCallback(t *testing.T) {
	origAttempts := util.DefaultRetryAttempts
	origInterval := util.DefaultRetryInterval
	util.DefaultRetryAttempts = 0
	util.DefaultRetryInterval = time.Millisecond
	defer func() {
		util.DefaultRetryAttempts = origAttempts
		util.DefaultRetryInterval = origInterval
	}()

	client := NewTenantWatchClient[testResource](
		"http://example.com",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]testResource, error) {
			return nil, nil
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)
	client.connectToWebSocket = func(ctx context.Context, tenantManagerUrl string, dialer *websocket.Dialer,
		authSupplier func(ctx context.Context) (string, error), onConnect func()) error {
		return errors.New("connection refused")
	}

	notified := make(chan error, 1)
	err := client.Watch(context.Background(),
		classifier.Keys{classifier.Name: "r1", classifier.Namespace: "ns"},
		func(resources []testResource, err error) {
			select {
			case notified <- err:
			default:
			}
		})
	require.Error(t, err)

	select {
	case callbackErr := <-notified:
		assert.Error(t, callbackErr)
	case <-time.After(2 * time.Second):
		t.Fatal("the failure never reached the callback")
	}
}

func waitForWatchers[T Resource](b *TenantWatchBroadcaster[T], count int) bool {
	for i := 0; i < 200; i++ {
		b.lock.RLock()
		registered := len(b.watchers)
		b.lock.RUnlock()
		if registered >= count {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
