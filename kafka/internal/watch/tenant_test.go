package watch

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
	go_stomp_websocket "github.com/netcracker/qubership-core-lib-go-stomp-websocket/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace = "test-namespace"
	timeout       = 10 * time.Second
)

func newMockSubscription() *go_stomp_websocket.Subscription {
	return &go_stomp_websocket.Subscription{
		FrameCh: make(chan *go_stomp_websocket.Frame, 10),
		Id:      "test-sub",
		Topic:   "/channels/tenants",
	}
}

type mockResource struct {
	Name string
}

func (m mockResource) GetClassifier() classifier.Keys {
	return classifier.Keys{}
}

func Test_mergeTenantsSubscribed(t *testing.T) {
	assertions := require.New(t)
	broadcaster := &TenantWatchBroadcaster[model.TopicAddress]{}
	tenantWatchEvent := &watch.TenantWatchEvent{
		Type: watch.SUBSCRIBED,
		Tenants: []watch.Tenant{
			{
				ExternalId: "1",
				Status:     watch.StatusActive,
				Name:       "name-1",
				Namespace:  testNamespace,
			},
			{
				ExternalId: "2",
				Status:     watch.StatusSuspended,
				Name:       "name-2",
				Namespace:  testNamespace,
			}},
	}
	changed := broadcaster.mergeTenants(tenantWatchEvent)
	assertions.True(changed)
	assertions.Equal(1, len(broadcaster.currentTenants))
	assertions.Equal("1", broadcaster.currentTenants[0].ExternalId)
}

func Test_mergeTenantsCreated(t *testing.T) {
	assertions := require.New(t)
	broadcaster := &TenantWatchBroadcaster[model.TopicAddress]{}
	tenantWatchEvent := &watch.TenantWatchEvent{
		Type: watch.CREATED,
		Tenants: []watch.Tenant{{
			ExternalId: "1",
			Status:     "CREATED",
			Name:       "name-1",
			Namespace:  testNamespace,
		}},
	}
	changed := broadcaster.mergeTenants(tenantWatchEvent)
	assertions.False(changed)
	assertions.Equal(0, len(broadcaster.currentTenants))
}

func Test_mergeTenantsModified(t *testing.T) {
	assertions := require.New(t)
	broadcaster := &TenantWatchBroadcaster[model.TopicAddress]{}

	tenantWatchEvent0 := &watch.TenantWatchEvent{
		Type: watch.SUBSCRIBED,
		Tenants: []watch.Tenant{{
			ExternalId: "1",
			Status:     watch.StatusActive,
			Name:       "name-1",
			Namespace:  testNamespace,
		}},
	}

	tenantWatchEvent1 := &watch.TenantWatchEvent{
		Type: watch.MODIFIED,
		Tenants: []watch.Tenant{{
			ExternalId: "1",
			Status:     "SUSPENDING",
			Name:       "name-1",
			Namespace:  testNamespace,
		}},
	}

	tenantWatchEvent2 := &watch.TenantWatchEvent{
		Type: watch.MODIFIED,
		Tenants: []watch.Tenant{{
			ExternalId: "1",
			Status:     watch.StatusSuspended,
			Name:       "name-1",
			Namespace:  testNamespace,
		}},
	}

	tenantWatchEvent3 := &watch.TenantWatchEvent{
		Type: watch.MODIFIED,
		Tenants: []watch.Tenant{{
			ExternalId: "1",
			Status:     "RESUMING",
			Name:       "name-1",
			Namespace:  testNamespace,
		}},
	}
	tenantWatchEvent4 := &watch.TenantWatchEvent{
		Type: watch.MODIFIED,
		Tenants: []watch.Tenant{{
			ExternalId: "1",
			Status:     watch.StatusActive,
			Name:       "name-1",
			Namespace:  testNamespace,
		}},
	}

	changed := broadcaster.mergeTenants(tenantWatchEvent0)
	assertions.True(changed)
	assertions.Equal(1, len(broadcaster.currentTenants))
	assertions.Equal("1", broadcaster.currentTenants[0].ExternalId)

	changed = broadcaster.mergeTenants(tenantWatchEvent1)
	assertions.False(changed)
	assertions.Equal(1, len(broadcaster.currentTenants))
	assertions.Equal("1", broadcaster.currentTenants[0].ExternalId)

	changed = broadcaster.mergeTenants(tenantWatchEvent2)
	assertions.True(changed)
	assertions.Equal(0, len(broadcaster.currentTenants))

	changed = broadcaster.mergeTenants(tenantWatchEvent3)
	assertions.False(changed)
	assertions.Equal(0, len(broadcaster.currentTenants))

	changed = broadcaster.mergeTenants(tenantWatchEvent4)
	assertions.True(changed)
	assertions.Equal(1, len(broadcaster.currentTenants))
	assertions.Equal("1", broadcaster.currentTenants[0].ExternalId)
}

func Test_mergeTenantsDeleted(t *testing.T) {
	assertions := require.New(t)
	broadcaster := &TenantWatchBroadcaster[model.TopicAddress]{}

	tenantWatchEvent0 := &watch.TenantWatchEvent{
		Type: watch.SUBSCRIBED,
		Tenants: []watch.Tenant{
			{
				ExternalId: "1",
				Status:     watch.StatusActive,
				Name:       "name-1",
				Namespace:  testNamespace,
			},
			{
				ExternalId: "2",
				Status:     watch.StatusActive,
				Name:       "name-2",
				Namespace:  testNamespace,
			},
		},
	}

	tenantWatchEvent1 := &watch.TenantWatchEvent{
		Type: watch.DELETED,
		Tenants: []watch.Tenant{
			{
				ExternalId: "1",
				Status:     "DELETED",
				Name:       "name-1",
				Namespace:  testNamespace,
			},
		},
	}
	changed := broadcaster.mergeTenants(tenantWatchEvent0)
	assertions.True(changed)
	assertions.Equal(2, len(broadcaster.currentTenants))
	assertions.Equal("1", broadcaster.currentTenants[0].ExternalId)

	changed = broadcaster.mergeTenants(tenantWatchEvent1)
	assertions.True(changed)
	assertions.Equal(1, len(broadcaster.currentTenants))
	assertions.Equal("2", broadcaster.currentTenants[0].ExternalId)
}

func Test_mergeTenantsReadEvents(t *testing.T) {
	assertions := require.New(t)
	broadcaster := &TenantWatchBroadcaster[model.TopicAddress]{
		tenants: make(chan []watch.Tenant),
	}
	ctx, cancel := context.WithCancel(context.Background())
	subscription := &go_stomp_websocket.Subscription{
		FrameCh: make(chan *go_stomp_websocket.Frame),
		Id:      "1",
		Topic:   "topic",
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := broadcaster.readEvents(ctx, subscription, func() {})
		assertions.True(errors.Is(err, context.Canceled))
		wg.Done()
	}()

	event := &watch.TenantWatchEvent{
		Type: watch.SUBSCRIBED,
		Tenants: []watch.Tenant{
			{
				ExternalId: "1",
				Status:     watch.StatusActive,
				Name:       "name-1",
				Namespace:  testNamespace,
			},
		},
	}

	eventJson, err := json.Marshal(event)
	assertions.NoError(err)

	subscription.FrameCh <- &go_stomp_websocket.Frame{
		Command: "MESSAGE",
		Headers: []string{},
		Body:    string(eventJson),
	}

	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		tenants := <-broadcaster.tenants
		assertions.Equal(1, len(tenants))
		assertions.Equal("1", tenants[0].ExternalId)
		wg2.Done()
	}()
	assertions.True(waitWithTimeout(wg2, timeout))

	cancel()

	assertions.True(waitWithTimeout(wg, timeout))
}

func Test_mergeTenantsReadEventsClosedSubscription(t *testing.T) {
	assertions := require.New(t)
	broadcaster := &TenantWatchBroadcaster[model.TopicAddress]{
		tenants: make(chan []watch.Tenant),
	}
	ctx := context.Background()
	subscription := &go_stomp_websocket.Subscription{
		FrameCh: make(chan *go_stomp_websocket.Frame),
		Id:      "1",
		Topic:   "topic",
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := broadcaster.readEvents(ctx, subscription, func() {})
		assertions.Equal(errors.New("closed frame channel"), err)
		wg.Done()
	}()

	close(subscription.FrameCh)

	assertions.True(waitWithTimeout(wg, timeout))
}

func Test_TenantWatchBroadcaster_mergeTenants_unknown_event(t *testing.T) {
	broadcaster := NewTenantWatchClient[model.TopicAddress](
		"http://localhost:1234",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]model.TopicAddress, error) {
			return nil, nil
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)
	event := &watch.TenantWatchEvent{
		Type:    "UNKNOWN",
		Tenants: []watch.Tenant{{ExternalId: "1", Status: watch.StatusActive}},
	}
	changed := broadcaster.mergeTenants(event)
	require.False(t, changed)
}

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return true // completed normally
	case <-time.After(timeout):
		return false // timed out
	}
}

// Fake Resource type for generic parameter
type fakeResource struct {
	ID string
}

func (f fakeResource) GetClassifier() classifier.Keys {
	return classifier.Keys{}
}

// Test mergeTenants logic
func TestMergeTenants_Subscribe_Modified_Deleted(t *testing.T) {
	assertions := require.New(t)

	b := &TenantWatchBroadcaster[fakeResource]{}

	subEvent := watch.TenantWatchEvent{
		Type: watch.SUBSCRIBED,
		Tenants: []watch.Tenant{
			{ExternalId: "1", Status: watch.StatusActive},
			{ExternalId: "2", Status: watch.StatusSuspended},
		},
	}
	changed := b.mergeTenants(&subEvent)
	assertions.True(changed, "expected tenants to change on SUBSCRIBED")
	assertions.Equal(1, len(b.currentTenants), "expected 1 active tenant")
	assertions.Equal("1", b.currentTenants[0].ExternalId, "expected tenant with ExternalId '1'")

	modEvent := watch.TenantWatchEvent{
		Type: watch.MODIFIED,
		Tenants: []watch.Tenant{
			{ExternalId: "3", Status: watch.StatusActive},
			{ExternalId: "1", Status: watch.StatusSuspended},
		},
	}
	b.mergeTenants(&modEvent)
	assertions.Len(b.currentTenants, 1, "expected 1 active tenant")

	delEvent := watch.TenantWatchEvent{
		Type: watch.DELETED,
		Tenants: []watch.Tenant{
			{ExternalId: "3"},
		},
	}
	b.mergeTenants(&delEvent)
	assertions.Len(b.currentTenants, 0, "expected 0 active tenants after deletion")
}

// Test filterTenants
func TestFilterTenants(t *testing.T) {
	assertions := require.New(t)
	tenants := []watch.Tenant{
		{ExternalId: "1", Status: watch.StatusActive},
		{ExternalId: "2", Status: watch.StatusSuspended},
	}
	active := filterTenants(tenants, func(tn watch.Tenant) bool {
		return tn.Status == watch.StatusActive
	})
	assertions.Len(active, 1)
	assertions.Equal("1", active[0].ExternalId)
}

// Test watcher processing (no panic)
func TestWatcherProcessSafe(t *testing.T) {
	called := false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &watcher[fakeResource]{
		name:      "n",
		namespace: "ns",
		userCtx:   ctx,
		callback: func(r []fakeResource, err error) {
			called = true
			panic("boom")
		},
	}
	defer func() { _ = recover() }()
	w.process([]fakeResource{{ID: "x"}}, nil)
	if !called {
		t.Fatal("callback was not called")
	}
}

func TestMergeTenants_Subscribed(t *testing.T) {
	b := &TenantWatchBroadcaster[mockResource]{}
	event := &watch.TenantWatchEvent{
		Type:    watch.SUBSCRIBED,
		Tenants: []watch.Tenant{makeTenant(watch.StatusActive)},
	}
	changed := b.mergeTenants(event)
	assert.True(t, changed)
	assert.Len(t, b.currentTenants, 1)
}

func TestMergeTenants_ModifiedAndDeleted(t *testing.T) {
	b := &TenantWatchBroadcaster[mockResource]{}
	b.currentTenants = []watch.Tenant{makeTenant(watch.StatusActive)}

	// Modified to suspended removes tenant
	event := &watch.TenantWatchEvent{
		Type:    watch.MODIFIED,
		Tenants: []watch.Tenant{makeTenant(watch.StatusSuspended)},
	}
	assert.True(t, b.mergeTenants(event))
	assert.Len(t, b.currentTenants, 0)

	// Deleted removes tenant
	b.currentTenants = []watch.Tenant{makeTenant(watch.StatusActive)}
	event = &watch.TenantWatchEvent{
		Type:    watch.DELETED,
		Tenants: []watch.Tenant{makeTenant(watch.StatusActive)},
	}
	assert.True(t, b.mergeTenants(event))
	assert.Len(t, b.currentTenants, 0)
}

func TestReadEvents_Success(t *testing.T) {
	sub := newMockSubscription()
	b := &TenantWatchBroadcaster[mockResource]{tenants: make(chan []watch.Tenant, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	onConnectCalled := false
	onConnect := func() { onConnectCalled = true }

	go func() {
		event := watch.TenantWatchEvent{
			Type:    watch.SUBSCRIBED,
			Tenants: []watch.Tenant{makeTenant(watch.StatusActive)},
		}
		data, _ := json.Marshal(event)
		sub.FrameCh <- &go_stomp_websocket.Frame{Body: string(data)}
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := b.readEvents(ctx, sub, onConnect)
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, onConnectCalled)
}

func TestMergeTenants_AllPaths(t *testing.T) {
	b := &TenantWatchBroadcaster[mockResource]{}

	// SUBSCRIBED
	e1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{makeTenant(watch.StatusActive)}}
	assert.True(t, b.mergeTenants(e1))
	assert.Len(t, b.currentTenants, 1)

	// CREATED (ignored)
	e2 := &watch.TenantWatchEvent{Type: watch.CREATED}
	assert.False(t, b.mergeTenants(e2))

	// MODIFIED: add suspended
	b.currentTenants = []watch.Tenant{makeTenant(watch.StatusActive)}
	e3 := &watch.TenantWatchEvent{Type: watch.MODIFIED, Tenants: []watch.Tenant{makeTenant(watch.StatusSuspended)}}
	assert.True(t, b.mergeTenants(e3))

	// DELETED
	b.currentTenants = []watch.Tenant{makeTenant(watch.StatusActive)}
	e4 := &watch.TenantWatchEvent{Type: watch.DELETED, Tenants: []watch.Tenant{makeTenant(watch.StatusActive)}}
	assert.True(t, b.mergeTenants(e4))
	assert.Empty(t, b.currentTenants)

	// UNKNOWN
	e5 := &watch.TenantWatchEvent{Type: "foo"}
	assert.False(t, b.mergeTenants(e5))
}

func makeTenant(status watch.TenantStatus) watch.Tenant {
	return watch.Tenant{
		ExternalId: "tenant1",
		Status:     status,
	}
}
