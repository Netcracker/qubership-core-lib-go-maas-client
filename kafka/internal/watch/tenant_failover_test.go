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

// Once the connection retry budget is exhausted, the broadcaster gives up and
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
