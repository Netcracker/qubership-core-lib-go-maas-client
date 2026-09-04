package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal"
	watchInternal "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal/watch"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
)

// maxWatchWindow is the longest wait asked of maas-service.
const maxWatchWindow = 60 * time.Second

// watchWindow keeps the server-side wait below the client timeout, so the
// server answers first instead of the client reporting a timeout.
func watchWindow(httpClient *resty.Client) time.Duration {
	timeout := httpClient.GetClient().Timeout
	if timeout <= 0 {
		return maxWatchWindow
	}
	return min(timeout-timeout/4, maxWatchWindow)
}

// NewClient builds a Kafka MaaS client. httpClient must not retry on its own:
// this client retries, and resty retries would multiply it.
func NewClient(namespace string, maasAgentUrl string, tenantManagerUrl string, httpClient *resty.Client,
	dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error)) MaasClient {
	crudClient := &internal.CrudClient{
		MaasAgentUrl:     maasAgentUrl,
		Namespace:        namespace,
		HttpClient:       httpClient,
		MaxTotalDuration: util.DefaultMaxTotalDuration,
		AttemptTimeout:   util.DefaultAttemptTimeout,
	}
	watchPath := fmt.Sprintf("/api/v2/kafka/topic/watch-create?timeout=%s", watchWindow(httpClient))
	watchClient := watchInternal.NewClient[model.TopicAddress](maasAgentUrl,
		watchPath, httpClient, internal.ResponseToTopicAddress)
	getResources := func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]model.TopicAddress, error) {
		var topics []model.TopicAddress
		for _, tenant := range tenants {
			keysWithTenant := keys.WithTenantId(tenant.ExternalId)
			topic, err := crudClient.GetTopic(ctx, keysWithTenant)
			if err != nil {
				return nil, err
			} else if topic != nil {
				topics = append(topics, *topic)
			}
		}
		return topics, nil
	}
	tenantWatchClient := watchInternal.NewTenantWatchClient[model.TopicAddress](tenantManagerUrl, getResources, dialer, authSupplier)
	return internal.NewKafkaClient(namespace, crudClient, watchClient, tenantWatchClient)
}
