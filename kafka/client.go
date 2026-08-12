package kafka

import (
	"context"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal"
	watchInternal "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal/watch"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
)

// NewClient builds a Kafka MaaS client.
//
// httpClient must not retry on its own: this client already retries, and resty
// retries multiply it, turning the configured attempts into their product.
// Pass a plain resty.New() or one with SetRetryCount(0).
func NewClient(namespace string, maasAgentUrl string, tenantManagerUrl string, httpClient *resty.Client,
	dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error)) MaasClient {
	crudClient := &internal.CrudClient{
		MaasAgentUrl:   maasAgentUrl,
		Namespace:      namespace,
		HttpClient:     httpClient,
		RetryInterval:  util.DefaultRetryInterval,
		RetryAttempts:  util.DefaultRetryAttempts,
		AttemptTimeout: util.DefaultAttemptTimeout,
	}
	watchClient := watchInternal.NewClient[model.TopicAddress](maasAgentUrl,
		"/api/v2/kafka/topic/watch-create?timeout=60s", httpClient, internal.ResponseToTopicAddress)
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
