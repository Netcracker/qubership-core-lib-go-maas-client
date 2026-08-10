package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal/watch"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
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
	MaasAgentUrl  string
	Namespace     string
	HttpClient    *resty.Client
	Auth          func(ctx context.Context) (string, error)
	RetryAttempts int
	RetryInterval time.Duration
}

func (d *CrudClient) GetOrCreateTopic(ctx context.Context, keys classifier.Keys, options ...model.TopicCreateOptions) (*model.TopicAddress, error) {
	var topicAddress *model.TopicAddress
	err := util.NewRetry(d.RetryAttempts, d.RetryInterval).RunCtx(ctx, func(ctx context.Context) error {
		var opt model.TopicCreateOptions
		if len(options) == 1 {
			opt = options[0]
		} else if len(options) > 1 {
			return fmt.Errorf("only 0 or 1 option is allowed")
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
		request := d.HttpClient.R().SetContext(ctx).SetBody(reqBody)
		err := d.addAuthToken(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to add auth token to request: %w", err)
		}
		if len(opt.OnTopicExists) > 0 {
			request.SetQueryParam(OnTopicExistsQueryParam, string(opt.OnTopicExists))
		}
		response, err := request.Post(d.MaasAgentUrl + "/api/v1/kafka/topic")
		if err != nil {
			return fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
		}
		logger.InfoC(ctx, "Received response: %d", response.StatusCode())
		if !response.IsSuccess() {
			return util.ClassifyResponseError(response.StatusCode(), response.Status(), response.String())
		}
		var TopicResponse TopicResponse
		body := response.Body()
		pErr := json.Unmarshal(body, &TopicResponse)
		if pErr != nil {
			return fmt.Errorf("failed to parse response from maas-agent. Cause: %w", pErr)
		}
		topicAddress, err = newTopicAddress(TopicResponse)
		if err != nil {
			return fmt.Errorf("failed to Convert response to TopicAddress. Cause: %w", err)
		}
		return nil
	})
	return topicAddress, err
}

func (d *CrudClient) GetTopic(ctx context.Context, keys classifier.Keys) (*model.TopicAddress, error) {
	var topicAddress *model.TopicAddress
	err := util.NewRetry(d.RetryAttempts, d.RetryInterval).RunCtx(ctx, func(ctx context.Context) error {
		logger.InfoC(ctx, "Get topic by classifier %v", keys)
		request := d.HttpClient.R().SetContext(ctx).SetBody(keys)
		err := d.addAuthToken(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to add auth token to request: %w", err)
		}
		response, err := request.Post(d.MaasAgentUrl + "/api/v1/kafka/topic/get-by-classifier")
		if err != nil {
			return fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
		}
		logger.InfoC(ctx, "Received response: %d", response.StatusCode())
		if response.StatusCode() == 404 {
			return nil
		}
		if !response.IsSuccess() {
			return util.ClassifyResponseError(response.StatusCode(), response.Status(), response.String())
		}
		var TopicResponse TopicResponse
		body := response.Body()
		pErr := json.Unmarshal(body, &TopicResponse)
		if pErr != nil {
			return fmt.Errorf("failed to parse response from maas-agent. Cause: %w", pErr)
		}
		topicAddress, err = newTopicAddress(TopicResponse)
		if err != nil {
			return fmt.Errorf("failed to Convert response to TopicAddress. Cause: %w", err)
		}
		return nil
	})
	return topicAddress, err
}

func (d *CrudClient) DeleteTopic(ctx context.Context, classifier classifier.Keys) error {
	return util.NewRetry(d.RetryAttempts, d.RetryInterval).RunCtx(ctx, func(ctx context.Context) error {
		body := TopicSearchRequest{Classifier: classifier}
		logger.InfoC(ctx, "Get or Create topic by classifier %v", classifier)
		request := d.HttpClient.R().SetContext(ctx).SetBody(body)
		err := d.addAuthToken(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to add auth token to request: %w", err)
		}
		response, err := request.Delete(d.MaasAgentUrl + "/api/v1/kafka/topic")
		if err != nil {
			return fmt.Errorf("failed to send request to maas-agent. Cause: %w", err)
		}
		logger.InfoC(ctx, "Received response: %d", response.StatusCode())
		if !response.IsSuccess() {
			return util.ClassifyResponseError(response.StatusCode(), response.Status(), response.String())
		}
		return nil
	})
}

func (d *CrudClient) addAuthToken(ctx context.Context, request *resty.Request) error {
	token, err := d.Auth(ctx)
	if err != nil {
		return fmt.Errorf("failed to get auth token: %w", err)
	}
	request.Header.Add("Authorization", "Bearer "+token)
	return nil
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
