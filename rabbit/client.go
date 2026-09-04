package rabbit

import (
	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/rabbit/internal"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
)

// NewClient builds a Rabbit MaaS client. httpClient must not retry on its own:
// this client retries, and resty retries would multiply it.
func NewClient(namespace string, maasAgentUrl string, httpClient *resty.Client) MaasClient {
	client := &internal.CrudClient{
		MaasAgentUrl:     maasAgentUrl,
		Namespace:        namespace,
		HttpClient:       httpClient,
		MaxTotalDuration: util.DefaultMaxTotalDuration,
		AttemptTimeout:   util.DefaultAttemptTimeout,
	}

	return internal.NewRabbitClient(namespace, client)
}
