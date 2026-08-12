package rabbit

import (
	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/rabbit/internal"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
)

// NewClient builds a Rabbit MaaS client.
//
// httpClient must not retry on its own: this client already retries, and resty
// retries multiply it, turning the configured attempts into their product.
// Pass a plain resty.New() or one with SetRetryCount(0).
func NewClient(namespace string, maasAgentUrl string, httpClient *resty.Client) MaasClient {
	client := &internal.CrudClient{
		MaasAgentUrl:   maasAgentUrl,
		Namespace:      namespace,
		HttpClient:     httpClient,
		RetryAttempts:  util.DefaultRetryAttempts,
		RetryInterval:  util.DefaultRetryInterval,
		AttemptTimeout: util.DefaultAttemptTimeout,
	}

	return internal.NewRabbitClient(namespace, client)
}
