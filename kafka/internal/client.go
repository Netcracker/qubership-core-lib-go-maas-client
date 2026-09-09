package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/internal/rest"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal/watch"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("maas-kafka-client")
}

const (
	OnTopicExistsQueryParam string = "onTopicExists"
)

type MaasClient struct {
	namespace         string
	crudClient        *CrudClient
	watchClient       watch.Client[model.TopicAddress]
	tenantWatchClient watch.TenantWatchClient[model.TopicAddress]
}

func NewKafkaClient(namespace string,
	crudClient *CrudClient, watchClient watch.Client[model.TopicAddress],
	tenantWatchClient watch.TenantWatchClient[model.TopicAddress]) *MaasClient {
	return &MaasClient{
		namespace:         namespace,
		crudClient:        crudClient,
		watchClient:       watchClient,
		tenantWatchClient: tenantWatchClient,
	}
}

func (c *MaasClient) GetOrCreateTopic(ctx context.Context, classifier classifier.Keys, options ...model.TopicCreateOptions) (*model.TopicAddress, error) {
	c.insureNamespacePresent(classifier)
	return c.crudClient.GetOrCreateTopic(ctx, classifier, options...)
}

func (c *MaasClient) GetTopic(ctx context.Context, classifier classifier.Keys) (*model.TopicAddress, error) {
	c.insureNamespacePresent(classifier)
	return c.crudClient.GetTopic(ctx, classifier)
}

func (c *MaasClient) DeleteTopic(ctx context.Context, classifier classifier.Keys) error {
	c.insureNamespacePresent(classifier)
	return c.crudClient.DeleteTopic(ctx, classifier)
}

func (c *MaasClient) WatchTenantTopics(ctx context.Context, classifier classifier.Keys, callback func([]model.TopicAddress)) error {
	c.insureNamespacePresent(classifier)
	wrapper := func(topics []model.TopicAddress, err error) {
		if err == nil {
			callback(topics)
		}
	}
	return c.tenantWatchClient.Watch(ctx, classifier, wrapper)
}

func (c *MaasClient) WatchTenantKafkaTopics(ctx context.Context, classifier classifier.Keys, callback func(topics []model.TopicAddress, err error)) error {
	c.insureNamespacePresent(classifier)
	return c.tenantWatchClient.Watch(ctx, classifier, callback)
}

func (c *MaasClient) WatchTopicCreate(ctx context.Context, classifier classifier.Keys, callback func(model.TopicAddress)) error {
	c.insureNamespacePresent(classifier)
	return c.watchClient.WatchOnCreateResources(ctx, classifier, callback)
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

func (d *CrudClient) GetOrCreateTopic(ctx context.Context, keys classifier.Keys, options ...model.TopicCreateOptions) (*model.TopicAddress, error) {
	if len(options) > 1 {
		return nil, fmt.Errorf("only 0 or 1 option is allowed")
	}
	var opt model.TopicCreateOptions
	if len(options) == 1 {
		opt = options[0]
	}
	reqBody := TopicRequest{
		Name:              opt.Name,
		Classifier:        keys,
		ExternallyManaged: opt.ExternallyManaged,
		NumPartitions:     opt.NumPartitions,
		ReplicationFactor: opt.ReplicationFactor,
		ReplicaAssignment: opt.ReplicaAssignment,
		Configs:           opt.Configs,
		Template:          opt.Template,
	}

	logger.InfoC(ctx, "Get or Create topic by classifier %v", keys)
	response, err := d.caller().Send(ctx, func(request *resty.Request) (*resty.Response, error) {
		request.SetBody(reqBody)
		if len(opt.OnTopicExists) > 0 {
			request.SetQueryParam(OnTopicExistsQueryParam, string(opt.OnTopicExists))
		}
		return request.Post(d.MaasAgentUrl + "/api/v1/kafka/topic")
	})
	if err != nil {
		return nil, err
	}
	topicResponse, err := rest.Decode[TopicResponse](response)
	if err != nil || topicResponse == nil {
		return nil, err
	}
	return newTopicAddress(*topicResponse)
}

func (d *CrudClient) GetTopic(ctx context.Context, keys classifier.Keys) (*model.TopicAddress, error) {
	logger.InfoC(ctx, "Get topic by classifier %v", keys)
	response, err := d.caller().Send(ctx, func(request *resty.Request) (*resty.Response, error) {
		return request.SetBody(keys).Post(d.MaasAgentUrl + "/api/v1/kafka/topic/get-by-classifier")
	}, http.StatusNotFound)
	if err != nil {
		return nil, err
	}
	topicResponse, err := rest.Decode[TopicResponse](response)
	if err != nil || topicResponse == nil {
		return nil, err
	}
	return newTopicAddress(*topicResponse)
}

// DeleteTopic is retried: it reports only an error, so a repeat of a delete
// whose response was lost answers the same as the first attempt.
func (d *CrudClient) DeleteTopic(ctx context.Context, keys classifier.Keys) error {
	logger.InfoC(ctx, "Delete topic by classifier %v", keys)
	_, err := d.caller().Send(ctx, func(request *resty.Request) (*resty.Response, error) {
		return request.SetBody(TopicSearchRequest{Classifier: keys}).Delete(d.MaasAgentUrl + "/api/v1/kafka/topic")
	})
	return err
}

func newTopicAddress(response TopicResponse) (*model.TopicAddress, error) {
	bootstrapServers := make(map[string][]string)
	if response.Addresses != nil {
		for protocol, addresses := range response.Addresses {
			bootstrapServers[protocol] = addresses
		}
	}
	clientCredentials := make(map[string]model.TopicUserCredentials)
	if response.Credentials != nil {
		for _, creds := range response.Credentials["client"] {
			theType := creds["type"]
			if typeAsStr, ok := theType.(string); ok && typeAsStr != "" {
				credentials := model.TopicUserCredentials{}
				if username, found := creds["username"]; found {
					credentials.Username = username.(string)
				}
				if password, found := creds["password"]; found {
					formatAndValue := strings.Split(password.(string), ":")
					if len(formatAndValue) == 2 && formatAndValue[0] == "plain" {
						credentials.Password = formatAndValue[1]
					} else {
						return nil, fmt.Errorf("unsupported encoding format specified in 'credential.client.password' field for type '%s'. "+
							"Field must has prefix - 'plain:'", theType)
					}
				}
				if clientKey, found := creds["clientKey"]; found {
					credentials.ClientKey = clientKey.(string)
				}
				if clientCert, found := creds["clientCert"]; found {
					credentials.ClientCert = clientCert.(string)
				}
				clientCredentials[typeAsStr] = credentials
			}
		}
	}
	topicAddress := &model.TopicAddress{
		Classifier:      response.Classifier,
		TopicName:       response.Name,
		BoostrapServers: bootstrapServers,
		Credentials:     clientCredentials,
		CACert:          response.CACert,
	}
	if response.ActualSettings != nil {
		topicAddress.NumPartitions = response.ActualSettings.NumPartitions
		topicAddress.Configs = response.ActualSettings.Configs
	}
	return topicAddress, nil
}

func ResponseToTopicAddress(response *resty.Response) ([]model.TopicAddress, error) {
	var TopicResponseBody []TopicResponse
	body := response.Body()
	err := json.Unmarshal(body, &TopicResponseBody)
	if err != nil {
		return nil, err
	}
	var topicAddresses []model.TopicAddress
	for _, topicDTO := range TopicResponseBody {
		address, tErr := newTopicAddress(topicDTO)
		if tErr != nil {
			return nil, tErr
		}
		topicAddresses = append(topicAddresses, *address)
	}
	return topicAddresses, nil
}
