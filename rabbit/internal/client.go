package internal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/rabbit/model"
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
	MaasAgentUrl string
	Namespace    string
	HttpClient   *resty.Client
}

// vhostRequest is the body /api/v1/rabbit/vhost expects. The neighbouring
// get-by-classifier endpoint takes the bare classifier instead.
type vhostRequest struct {
	Classifier classifier.Keys `json:"classifier"`
}

func (d *CrudClient) GetOrCreateVhost(ctx context.Context, classifier classifier.Keys) (*model.Vhost, error) {
	logger.InfoC(ctx, "Get or Create vhost by classifier %v", classifier)
	request := d.HttpClient.R().SetContext(ctx).SetBody(vhostRequest{Classifier: classifier})

	response, err := request.Post(d.MaasAgentUrl + "/api/v1/rabbit/vhost")
	if err != nil {
		return nil, fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
	}
	logger.InfoC(ctx, "Received response: %d", response.StatusCode())
	if !response.IsSuccess() {
		return nil, fmt.Errorf("response with error code reveived. Status: %s, body: %s", response.Status(), response.String())
	}
	var vhost model.Vhost
	body := response.Body()
	pErr := json.Unmarshal(body, &vhost)
	if pErr != nil {
		return nil, fmt.Errorf("failed to parse response from maas-agent. Cause: %w", pErr)
	}

	return &vhost, nil
}

func (d *CrudClient) GetVhost(ctx context.Context, classifier classifier.Keys) (*model.VhostConfig, error) {
	logger.InfoC(ctx, "Get vhost by classifier %v", classifier)
	request := d.HttpClient.R().SetContext(ctx).SetBody(classifier)

	response, err := request.Post(d.MaasAgentUrl + "/api/v1/rabbit/vhost/get-by-classifier")
	if err != nil {
		return nil, fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
	}
	logger.InfoC(ctx, "Received response: %d", response.StatusCode())
	if !response.IsSuccess() {
		if response.StatusCode() == 404 {
			return nil, nil
		}
		return nil, fmt.Errorf("response with error code received. Status: %s, body: %s", response.Status(), response.String())
	}
	var vhost model.VhostConfig
	body := response.Body()
	pErr := json.Unmarshal(body, &vhost)
	if pErr != nil {
		return nil, fmt.Errorf("failed to parse response from maas-agent. Cause: %w", pErr)
	}

	return &vhost, nil
}

func (d *CrudClient) BuildHeaders(ctxData map[string]string) amqp.Table {
	result := amqp.Table{}

	for k, v := range ctxData {
		result[k] = v
	}
	return result
}
