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
