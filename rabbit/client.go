package rabbit

import (
	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/rabbit/internal"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
)

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
