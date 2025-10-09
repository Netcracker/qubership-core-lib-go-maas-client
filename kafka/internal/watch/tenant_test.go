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
	"github.com/stretchr/testify/require"
)

const (
	testNamespace = "test-namespace"
	timeout       = 10 * time.Second
)

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

func Test_TenantWatchBroadcaster_notifyWatchers_error(t *testing.T) {
	broadcaster := NewTenantWatchClient[model.TopicAddress](
		"http://localhost:1234",
		func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]model.TopicAddress, error) {
			return nil, errors.New("fail")
		},
		nil,
		func(ctx context.Context) (string, error) { return "token", nil },
	)
	broadcaster.currentTenants = []watch.Tenant{{ExternalId: "1", Status: watch.StatusActive, Name: "topic1", Namespace: "ns"}}
	broadcaster.tenants = make(chan []watch.Tenant, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var called bool
	broadcaster.Watch(ctx, classifier.New("topic1").WithNamespace("ns"), func(resources []model.TopicAddress, err error) {
		called = true
	})

	// Should not panic or deadlock
	broadcaster.notifyWatchers(broadcaster.currentTenants)
	require.False(t, called)
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
