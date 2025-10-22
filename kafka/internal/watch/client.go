package watch

import (
	"context"
	"errors"
	"sync"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("maas-kafka-watch")
}

type Resource interface {
	GetClassifier() classifier.Keys
}

type Client[T Resource] interface {
	WatchTenantResources(ctx context.Context, classifier classifier.Keys, callback func([]T)) error
	WatchOnCreateResources(ctx context.Context, classifier classifier.Keys, callback func(T)) error
}

func NewClient[T Resource](maasAgentUrl string, watchPath string,
	httpClient *resty.Client, responseToResources func(response *resty.Response) ([]T, error)) *DefaultClient[T] {
	return &DefaultClient[T]{
		watchUrl:   maasAgentUrl + watchPath,
		httpClient: httpClient,
		converter:  responseToResources,
		watchLock:  &sync.RWMutex{},
	}
}

type DefaultClient[T Resource] struct {
	watchUrl    string
	httpClient  *resty.Client
	watchCancel func() []*watchHolder[T]
	watchLock   *sync.RWMutex
	converter   func(response *resty.Response) ([]T, error)
}

func (d *DefaultClient[T]) WatchTenantResources(ctx context.Context, keys classifier.Keys, callback func([]T)) error {
	panic("not supported yet")
}

func (d *DefaultClient[T]) WatchOnCreateResources(ctx context.Context, keys classifier.Keys, callback func(T)) error {
	// Changed to buffered channel to prevent blocking on send.
	watchersChan := make(chan []*watchHolder[T], 1)
	ctx, cancelFunc := context.WithCancel(ctx)
	prevWatchHolders := d.cancelWatchIfAnyAndSet(func() []*watchHolder[T] {
		cancelFunc()
		return <-watchersChan
	})
	watchHolders := append(prevWatchHolders, &watchHolder[T]{
		classifier: keys,
		callback:   callback,
	})
	go func(ctx context.Context, watchHolders []*watchHolder[T], watchersChan chan []*watchHolder[T]) {
		// reset cancel func when we processed all classifiers
		defer d.clearWatchCancel()
		for len(watchHolders) > 0 {
			var classifiers []classifier.Keys
			for _, wHolder := range watchHolders {
				classifiers = append(classifiers, wHolder.classifier)
			}
			select {
			case <-ctx.Done():
				// we were cancelled by new watch request, return current classifiers back to watchersChan channel
				watchersChan <- watchHolders
				return
			default:
				response, err := d.httpClient.R().SetContext(ctx).SetBody(classifiers).Post(d.watchUrl)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						watchersChan <- watchHolders
						return
					} else {
						logger.Error("failed to send request to maas-agent. Cause: %w", err)
					}
				} else {
					watchHolders = d.processWatchResponse(watchHolders, func(holders []*watchHolder[T]) (result []*watchHolder[T]) {
						result = holders
						if !response.IsSuccess() {
							logger.Error("response with error code reveived. Status: %s, body: %s", response.Status(), response.String())
							return
						}
						resources, cErr := d.converter(response)
						if cErr != nil {
							logger.Error("failed to convert response. Cause: %w", cErr)
							return
						}
						for _, resource := range resources {
							var wHolderForTopic *watchHolder[T]
							for _, wHolder := range result {
								topicClassifier := resource.GetClassifier().AsString()
								holderClassifier := wHolder.classifier.AsString()
								if topicClassifier == holderClassifier {
									wHolderForTopic = wHolder
									break
								}
							}
							if wHolderForTopic != nil {
								d.executeCallback(resource, wHolderForTopic.callback)
								result = remove(result, wHolderForTopic)
							}
						}
						return
					})
				}
			}
		}
	}(ctx, watchHolders, watchersChan)
	return nil
}

func (d *DefaultClient[T]) executeCallback(resource T, callback func(T)) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("callback failed with panic: %s. Recovered", r)
		}
	}()
	callback(resource)
}

func (d *DefaultClient[T]) clearWatchCancel() {
	d.watchLock.Lock()
	defer d.watchLock.Unlock()
	d.watchCancel = nil
}

func (d *DefaultClient[T]) cancelWatchIfAnyAndSet(cancelFunc func() []*watchHolder[T]) []*watchHolder[T] {
	d.watchLock.Lock()
	defer d.watchLock.Unlock()
	var holders []*watchHolder[T]
	if d.watchCancel != nil {
		holders = d.watchCancel()
	}
	d.watchCancel = cancelFunc
	return holders
}

// need to process response under lock to make sure new Watch request do not cancel processing of already received response
// otherwise new request will process the same classifiers again
func (d *DefaultClient[T]) processWatchResponse(watchHolders []*watchHolder[T],
	resultF func(watchHolders []*watchHolder[T]) []*watchHolder[T]) []*watchHolder[T] {
	d.watchLock.Lock()
	defer d.watchLock.Unlock()
	return resultF(watchHolders)
}

func (d *DefaultClient[T]) createWatchChan(ctx context.Context, classifiers []classifier.Keys, restFunc func(ctx context.Context, classifiers []classifier.Keys) (*resty.Response, error)) (chan *resty.Response, chan error) {
	respChan := make(chan *resty.Response)
	errChan := make(chan error)
	go func() {
		response, err := restFunc(ctx, classifiers)
		if err != nil {
			errChan <- err
		} else {
			respChan <- response
		}
	}()
	return respChan, errChan
}

func remove[T comparable](slice []T, target T) []T {
	// make a copy to prevent side effects on the original slice
	newSlice := append([]T(nil), slice...)
	i := -1
	for j, v := range newSlice {
		if v == target {
			i = j
			break
		}
	}
	if i != -1 {
		return append(newSlice[:i], newSlice[i+1:]...)
	}
	return newSlice
}

type watchHolder[T Resource] struct {
	classifier classifier.Keys
	callback   func(T)
}
