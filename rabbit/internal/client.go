package internal

import (
	"context"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/internal/rest"
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
	// MaxTotalDuration bounds a whole call, retries included.
	MaxTotalDuration time.Duration
	// AttemptTimeout bounds one request.
	AttemptTimeout time.Duration
}

// caller runs one CRUD call under this client's timeouts.
func (d *CrudClient) caller() rest.Caller {
	return rest.Caller{
		HttpClient:       d.HttpClient,
		Logger:           logger,
		MaxTotalDuration: d.MaxTotalDuration,
		AttemptTimeout:   d.AttemptTimeout,
	}
}

func (d *CrudClient) GetOrCreateVhost(ctx context.Context, classifier classifier.Keys) (*model.Vhost, error) {
	logger.InfoC(ctx, "Get or Create vhost by classifier %v", classifier)
	response, err := d.caller().Send(ctx, func(request *resty.Request) (*resty.Response, error) {
		return request.SetBody(classifier).Post(d.MaasAgentUrl + "/api/v1/rabbit/vhost")
	})
	if err != nil {
		return nil, err
	}
	return rest.Decode[model.Vhost](response)
}

func (d *CrudClient) GetVhost(ctx context.Context, classifier classifier.Keys) (*model.VhostConfig, error) {
	logger.InfoC(ctx, "Get vhost by classifier %v", classifier)
	response, err := d.caller().Send(ctx, func(request *resty.Request) (*resty.Response, error) {
		return request.SetBody(classifier).Post(d.MaasAgentUrl + "/api/v1/rabbit/vhost/get-by-classifier")
	}, http.StatusNotFound)
	if err != nil {
		return nil, err
	}
	return rest.Decode[model.VhostConfig](response)
}

func (d *CrudClient) BuildHeaders(ctxData map[string]string) amqp.Table {
	result := amqp.Table{}

	for k, v := range ctxData {
		result[k] = v
	}
	return result
}
