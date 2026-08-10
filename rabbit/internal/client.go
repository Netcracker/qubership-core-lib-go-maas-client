package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/rabbit/model"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	amqp "github.com/rabbitmq/amqp091-go"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("maas-rabbit-client")
}

type MaasClient struct {
	namespace  string
	crudClient *CrudClient
}

func NewRabbitClient(namespace string, crudClient *CrudClient) *MaasClient {
	return &MaasClient{
		namespace:  namespace,
		crudClient: crudClient,
	}
}

func (c *MaasClient) GetOrCreateVhost(ctx context.Context, classifier classifier.Keys) (*model.Vhost, error) {
	c.insureNamespacePresent(classifier)
	return c.crudClient.GetOrCreateVhost(ctx, classifier)
}

func (c *MaasClient) GetVhost(ctx context.Context, classifier classifier.Keys) (*model.VhostConfig, error) {
	c.insureNamespacePresent(classifier)
	return c.crudClient.GetVhost(ctx, classifier)
}

func (c *MaasClient) BuildHeaders(ctxData map[string]string) amqp.Table {
	return c.crudClient.BuildHeaders(ctxData)
}

func (c *MaasClient) insureNamespacePresent(keys classifier.Keys) {
	if _, found := keys[classifier.Namespace]; !found {
		keys.WithNamespace(c.namespace)
	}
}

type CrudClient struct {
	MaasAgentUrl  string
	Namespace     string
	HttpClient    *resty.Client
	RetryAttempts int
	RetryInterval time.Duration
}

// retry wraps CRUD calls; util.Retry itself falls back to util.Default*
// when RetryAttempts/RetryInterval are left unset.
func (d *CrudClient) retry() *util.Retry {
	return util.NewRetry(d.RetryAttempts, d.RetryInterval)
}

func (d *CrudClient) GetOrCreateVhost(ctx context.Context, classifier classifier.Keys) (*model.Vhost, error) {
	var result model.Vhost
	err := d.retry().RunCtx(ctx, func(ctx context.Context) error {
		logger.InfoC(ctx, "Get or Create vhost by classifier %v", classifier)
		request := d.HttpClient.R().SetContext(ctx).SetBody(classifier)

		response, err := request.Post(d.MaasAgentUrl + "/api/v1/rabbit/vhost")
		if err != nil {
			return fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
		}
		logger.InfoC(ctx, "Received response: %d", response.StatusCode())
		if !response.IsSuccess() {
			return util.ClassifyResponseError(response.StatusCode(), response.Status(), response.String())
		}
		body := response.Body()
		var vhost model.Vhost
		pErr := json.Unmarshal(body, &vhost)
		if pErr != nil {
			return fmt.Errorf("failed to parse response from maas-agent. Cause: %w", pErr)
		}
		result = vhost
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *CrudClient) GetVhost(ctx context.Context, classifier classifier.Keys) (*model.VhostConfig, error) {
	var result model.VhostConfig
	var notFound bool
	err := d.retry().RunCtx(ctx, func(ctx context.Context) error {
		logger.InfoC(ctx, "Get vhost by classifier %v", classifier)
		request := d.HttpClient.R().SetContext(ctx).SetBody(classifier)

		response, err := request.Post(d.MaasAgentUrl + "/api/v1/rabbit/vhost/get-by-classifier")
		if err != nil {
			return fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
		}
		logger.InfoC(ctx, "Received response: %d", response.StatusCode())
		if !response.IsSuccess() {
			if response.StatusCode() == 404 {
				notFound = true
				return nil
			}
			return util.ClassifyResponseError(response.StatusCode(), response.Status(), response.String())
		}
		body := response.Body()
		var vhost model.VhostConfig
		pErr := json.Unmarshal(body, &vhost)
		if pErr != nil {
			return fmt.Errorf("failed to parse response from maas-agent. Cause: %w", pErr)
		}
		result = vhost
		return nil
	})
	if err != nil {
		return nil, err
	}
	if notFound {
		return nil, nil
	}
	return &result, nil
}

func (d *CrudClient) BuildHeaders(ctxData map[string]string) amqp.Table {
	result := amqp.Table{}

	for k, v := range ctxData {
		result[k] = v
	}
	return result
}
