package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
	go_stomp_websocket "github.com/netcracker/qubership-core-lib-go-stomp-websocket/v3"
)

type TenantWatchClient[T Resource] interface {
	Watch(ctx context.Context, classifier classifier.Keys, callback func([]T, error)) error
}

func NewTenantWatchClient[T Resource](tenantManagerUrl string,
	getResources func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]T, error),
	dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error)) *TenantWatchBroadcaster[T] {
	tenantWatchBroadcaster := &TenantWatchBroadcaster[T]{
		watchers:         []*watcher[T]{},
		tenants:          make(chan []watch.Tenant),
		lock:             &sync.RWMutex{},
		startOnce:        &sync.Once{},
		getResources:     getResources,
		tenantManagerUrl: tenantManagerUrl,
		dialer:           dialer,
		authSupplier:     authSupplier,
		watchCounter:     0,
	}
	tenantWatchBroadcaster.connectToWebSocket = func(ctx context.Context, tenantManagerUrl string,
		dialer *websocket.Dialer,
		authSupplier func(ctx context.Context) (string, error),
		onConnect func()) error {
		return tenantWatchBroadcaster.connectToWS(ctx, tenantManagerUrl, dialer, authSupplier, onConnect)
	}
	return tenantWatchBroadcaster
}

type TenantWatchBroadcaster[T Resource] struct {
	watchers           []*watcher[T]
	internalProcCtx    context.Context
	currentTenants     []watch.Tenant
	tenants            chan []watch.Tenant
	lock               *sync.RWMutex
	cancel             context.CancelFunc
	startOnce          *sync.Once
	getResources       func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]T, error)
	tenantManagerUrl   string
	dialer             *websocket.Dialer
	authSupplier       func(ctx context.Context) (string, error)
	watchCounter       int
	connectToWebSocket func(ctx context.Context, tenantManagerUrl string, dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error), onConnect func()) error
}

func (b *TenantWatchBroadcaster[T]) start() error {
	logger.Info("Starting tenant watch broadcaster")
	ctx, cancel := context.WithCancel(context.Background())
	b.lock.Lock()
	b.internalProcCtx = ctx
	b.cancel = cancel
	b.lock.Unlock()

	readyChan := make(chan error, 1)
	go b.processLoop(ctx)
	go func() {
		var err error
		defer func() {
			// give up: clean up watchers and reset startOnce before letting the
			// blocked Watch() call return, so a subsequent Watch() reliably
			// starts a fresh connection instead of racing this cleanup.
			b.removeAllWatchersAndStop()
			select {
			case readyChan <- err:
			default:
			}
		}()
		retries := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				onConnect := func() {
					retries = 0 // reset retries on each successful connect
					select {
					case readyChan <- nil:
					default:
					}
				}
				err = b.connectToWebSocket(ctx, b.tenantManagerUrl, b.dialer, b.authSupplier, onConnect)
				if err != nil {
					if errors.Is(err, context.Canceled) || retries >= util.DefaultRetryAttempts {
						return
					}
					retries++
					duration := time.Duration(int32(retries)) * util.DefaultRetryInterval
					logger.ErrorC(ctx, "failed to connect to tenant manager web socket due to: %v, \nretrying after %f seconds", err, duration.Seconds())
					time.Sleep(duration)
					continue
				}
			}
		}
	}()
	select {
	case err := <-readyChan:
		return err
	}
}

func (b *TenantWatchBroadcaster[T]) stop() {
	logger.Info("Stopping tenant watch broadcaster")
	// reset once in case new watcher will be added
	b.cancel()
	b.startOnce = &sync.Once{}
}

// Watch registers a callback and returns once the shared connection either
// comes up or exhausts its retries. A nil return does not guarantee the
// connection stays alive afterwards: a watcher attaching to a round that is
// already giving up still receives the failure through its callback.
func (b *TenantWatchBroadcaster[T]) Watch(ctx context.Context, classifierKey classifier.Keys, callback func([]T, error)) error {
	name := classifierKey[classifier.Name]
	namespace := classifierKey[classifier.Namespace]
	if name == "" || namespace == "" {
		return errors.New("classifier must contain both name and namespace")
	}
	if _, present := classifierKey[classifier.TenantId]; present {
		return fmt.Errorf("classifier cannot contain '%s' param", classifier.TenantId)
	}

	b.lock.Lock()
	watchId := b.watchCounter
	b.watchCounter++
	userCtx, cancelUserCtx := context.WithCancel(ctx)
	logger.InfoC(userCtx, "Starting watcher#%d with name=%s, namespace=%s", watchId, name, namespace)
	w := watcher[T]{
		id:          watchId,
		name:        name,
		namespace:   namespace,
		callback:    callback,
		userCtx:     userCtx,
		cancel:      cancelUserCtx,
		queue:       make(chan []T, 1),
		broadcaster: b,
	}
	b.watchers = append(b.watchers, &w)
	// capture the current startOnce before releasing the lock: start() can
	// block for a long time and must not be called while holding it.
	startOnce := b.startOnce
	b.lock.Unlock()

	var err error
	startOnce.Do(func() {
		err = b.start()
	})

	// bind this watcher to whichever round's context is active now, instead
	// of reading the broadcaster's mutable field later from another
	// goroutine, which would race with a subsequent round replacing it.
	b.lock.RLock()
	w.procCtx = b.internalProcCtx
	b.lock.RUnlock()

	go w.run()
	logger.InfoC(userCtx, "Started watcher#%d", watchId)
	// re-enqueue tenants so broadcaster can re-sent tenants to the just added watcher
	go func() {
		b.lock.RLock()
		tenants := b.currentTenants
		b.lock.RUnlock()
		b.tenants <- tenants
	}()
	return err
}

func (b *TenantWatchBroadcaster[T]) connectToWS(ctx context.Context, tenantManagerUrl string,
	dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error), onConnect func()) error {
	parsedUrl, err := url.Parse(tenantManagerUrl + "/api/v4/tenant-manager/watch")
	if err != nil {
		return fmt.Errorf("failed not parse websocket URL %s. Cause: %w", tenantManagerUrl, err)
	}
	logger.InfoC(ctx, "Connecting to tenant manager %s", parsedUrl.String())
	auth, err := authSupplier(ctx)
	if err != nil {
		return fmt.Errorf("failed to get auth token: %w", err)
	}
	tmWatchClient, err := go_stomp_websocket.ConnectWithToken(*parsedUrl, *dialer, auth)
	if err != nil {
		return fmt.Errorf("failed to connect to %v: %w", parsedUrl.String(), err)
	}
	logger.InfoC(ctx, "Connected to tenant manager %s", parsedUrl.String())
	subscription, err := tmWatchClient.Subscribe("/channels/tenants")
	if err != nil {
		return fmt.Errorf("failed to subscribe to %v: %w", parsedUrl.String(), err)
	}
	return b.readEvents(ctx, subscription, onConnect)
}

func (b *TenantWatchBroadcaster[T]) readEvents(ctx context.Context, subscription *go_stomp_websocket.Subscription, onConnect func()) error {
	connected := false
	timeout := time.Duration(util.DefaultRetryAttempts) * util.DefaultRetryInterval
	for {
		var waitChan <-chan time.Time
		if !connected {
			timer := time.NewTimer(timeout)
			waitChan = timer.C
		} else {
			waitChan = make(<-chan time.Time) // never ending wait
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitChan:
			return fmt.Errorf("failed to wait for watcher to receive %s event from tenant-manager after: %s", watch.SUBSCRIBED, timeout)
		case frame, ok := <-subscription.FrameCh:
			if !ok {
				return errors.New("closed frame channel")
			}
			var event watch.TenantWatchEvent
			err := json.Unmarshal([]byte(frame.Body), &event)
			if err != nil {
				return fmt.Errorf("failed to unmarshal frame: %+v", frame)
			}
			if event.Type == watch.SUBSCRIBED {
				connected = true
				onConnect()
			}
			// merge received tenants with current ones
			changed := b.mergeTenants(&event)
			if changed {
				logger.InfoC(ctx, "Broadcasting updated active tenant list: %v", b.currentTenants)
				b.tenants <- b.currentTenants
			}
		}
	}
}

func (b *TenantWatchBroadcaster[T]) removeAllWatchersAndStop() {
	b.lock.Lock()
	defer b.lock.Unlock()
	logger.InfoC(b.internalProcCtx, "Removing all watchers")
	b.watchers = nil
	b.stop()
}

func (b *TenantWatchBroadcaster[T]) removeWatcher(w *watcher[T]) {
	b.lock.Lock()
	defer b.lock.Unlock()

	logger.InfoC(w.userCtx, "Removing watcher with name=%s, namespace=%s", w.name, w.namespace)
	var result []*watcher[T]
	for _, wi := range b.watchers {
		if wi != w {
			result = append(result, wi)
		}
	}
	b.watchers = result
	if len(b.watchers) == 0 {
		b.stop()
	}
}

func (b *TenantWatchBroadcaster[T]) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tenants := <-b.tenants:
			b.notifyWatchers(tenants)
		}
	}
}

func (b *TenantWatchBroadcaster[T]) notifyWatchers(tenants []watch.Tenant) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, w := range b.watchers {
		if reflect.DeepEqual(w.lastNotifiedTenants, tenants) {
			continue
		}
		keys := classifier.New(w.name).WithNamespace(w.namespace)
		resources, err := b.getResources(w.userCtx, keys, tenants)
		if err != nil {
			logger.ErrorC(b.internalProcCtx, "Failed to get resources: %s", err.Error())
			w.stop()
		} else if len(resources) > 0 {
			w.queue <- resources
			w.lastNotifiedTenants = tenants
		}
	}
}

func (b *TenantWatchBroadcaster[T]) mergeTenants(event *watch.TenantWatchEvent) bool {
	switch event.Type {
	case watch.SUBSCRIBED:
		logger.Info("Subscription event received. Tenants: %v", event.Tenants)
		b.currentTenants = filterTenants(event.Tenants, func(tenant watch.Tenant) bool {
			return tenant.Status == watch.StatusActive
		})
		return true
	case watch.CREATED:
		// ignore this event. we are interested only in ACTIVE tenants
		return false
	case watch.MODIFIED:
		logger.Info("Modify event received: Tenants: %v", event.Tenants)
		changedTenants := filterTenants(event.Tenants, func(tenant watch.Tenant) bool {
			return tenant.Status == watch.StatusSuspended || tenant.Status == watch.StatusActive
		})
		if len(changedTenants) == 0 {
			return false
		} else {
			var result []watch.Tenant
			for _, current := range b.currentTenants {
				add := true
				for _, changed := range changedTenants {
					if changed.Status == watch.StatusSuspended && current.ExternalId == changed.ExternalId {
						add = false
						break
					}
				}
				if add {
					result = append(result, current)
				}
			}
			for _, changed := range changedTenants {
				if changed.Status == watch.StatusActive {
					result = append(result, changed)
				}
			}
			b.currentTenants = result
			return true
		}
	case watch.DELETED:
		logger.Info("Deleted event received: Tenants: %v", event.Tenants)
		var result []watch.Tenant
		for _, current := range b.currentTenants {
			add := true
			for _, deleted := range event.Tenants {
				if current.ExternalId == deleted.ExternalId {
					add = false
					break
				}
			}
			if add {
				result = append(result, current)
			}
		}
		b.currentTenants = result
		return true
	default:
		logger.Errorf("Unknown event received. Event: %+v", event)
		return false
	}
}

func filterTenants(tenants []watch.Tenant, predicate func(tenant watch.Tenant) bool) []watch.Tenant {
	var activeTenants []watch.Tenant
	for _, t := range tenants {
		if predicate(t) {
			activeTenants = append(activeTenants, t)
		}
	}
	return activeTenants
}

type watcher[T Resource] struct {
	id                  int
	name                string
	namespace           string
	callback            func([]T, error)
	userCtx             context.Context
	cancel              context.CancelFunc
	queue               chan []T
	broadcaster         *TenantWatchBroadcaster[T]
	lastNotifiedTenants []watch.Tenant
	procCtx             context.Context
}

func (w *watcher[T]) process(resources []T, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorC(w.userCtx, "Callback failed with panic: %v, recovered", r)
		}
	}()
	logger.DebugC(w.userCtx, "Executing callback of watcher with name=%s, namespace=%s", w.name, w.namespace)
	w.callback(resources, err)
}

func (w *watcher[T]) run() {
	for {
		select {
		case <-w.procCtx.Done():
			// internal watcher process was canceled, we need to stop and notify all clients
			var empty []T
			w.process(empty, w.procCtx.Err())
			return
		case <-w.userCtx.Done():
			w.broadcaster.removeWatcher(w)
			var empty []T
			w.process(empty, w.userCtx.Err())
			return
		case resources := <-w.queue:
			w.process(resources, nil)
		}
	}
}

func (w *watcher[T]) stop() {
	logger.DebugC(w.userCtx, "Stopping watcher: %d", w.id)
	w.cancel()
}
