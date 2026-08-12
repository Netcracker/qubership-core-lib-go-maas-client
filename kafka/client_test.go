package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/watch"
	goStompWs "github.com/netcracker/qubership-core-lib-go-stomp-websocket/v3"
	"github.com/stretchr/testify/require"
)

const testNamespace = "test-namespace"

//var timeout = 5 * time.Second

var timeout = 5 * time.Minute
var testTokenValue = "test-token"

func Test_PLAINTEXT_PLAIN(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/PLAINTEXT_PLAIN.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"PLAINTEXT"}, topicAddress.GetProtocols())
		assertions.Equal([]string{"PLAIN"}, topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"server1:9092", "server2:9092"}, topicAddress.GetBoostrapServers("PLAINTEXT"))
		credentials := topicAddress.GetCredentials("PLAIN")
		assertions.NotNil(credentials)
		assertions.Equal("client", credentials.Username)
		assertions.Equal("client", credentials.Password)
	})
}

func Test_PLAINTEXT_SCRAM(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/PLAINTEXT_SCRAM.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"PLAINTEXT"}, topicAddress.GetProtocols())
		assertions.Equal([]string{"SCRAM"}, topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"server1:9092", "server2:9092"}, topicAddress.GetBoostrapServers("PLAINTEXT"))
		credentials := topicAddress.GetCredentials("SCRAM")
		assertions.NotNil(credentials)
		assertions.Equal("client", credentials.Username)
		assertions.Equal("client", credentials.Password)
	})
}

func Test_SASL_PLAINTEXT(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/SASL_PLAINTEXT_SCRAM.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"SASL_PLAINTEXT"}, topicAddress.GetProtocols())
		assertions.Equal([]string{"SCRAM"}, topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"kafka.test-kafka:9092"}, topicAddress.GetBoostrapServers("SASL_PLAINTEXT"))
		credentials := topicAddress.GetCredentials("SCRAM")
		assertions.NotNil(credentials)
		assertions.Equal("client", credentials.Username)
		assertions.Equal("client", credentials.Password)
	})
}

func Test_SASL_SSL_PLAIN_CLIENT_CERTS(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/SASL_SSL_PLAIN_CLIENT_CERTS.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"PLAINTEXT", "SASL_SSL"}, topicAddress.GetProtocols())
		assertions.Equal([]string{"sslCert+plain"}, topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"localkafka.kafka-cluster:9092"}, topicAddress.GetBoostrapServers("PLAINTEXT"))
		assertions.Equal([]string{"localkafka.kafka-cluster:9094"}, topicAddress.GetBoostrapServers("SASL_SSL"))
		credentials := topicAddress.GetCredentials("sslCert+plain")
		assertions.NotNil(credentials)
		assertions.Equal("alice", credentials.Username)
		assertions.Equal("alice-secret", credentials.Password)
		assertions.Equal("test-clientCert", credentials.ClientCert)
		assertions.Equal("test-clientKey", credentials.ClientKey)
	})
}

func Test_SASL_SSL_SCRAM(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/SASL_SSL_SCRAM.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"PLAINTEXT", "SASL_SSL"}, topicAddress.GetProtocols())
		assertions.Equal([]string{"SCRAM"}, topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"localkafka.kafka-cluster:9092"}, topicAddress.GetBoostrapServers("PLAINTEXT"))
		assertions.Equal([]string{"localkafka.kafka-cluster:9094"}, topicAddress.GetBoostrapServers("SASL_SSL"))
		credentials := topicAddress.GetCredentials("SCRAM")
		assertions.NotNil(credentials)
		assertions.Equal("alice", credentials.Username)
		assertions.Equal("alice-secret", credentials.Password)
	})
}

func Test_SASL_SSL_SCRAM_CLIENT_CERTS(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/SASL_SSL_SCRAM_CLIENT_CERTS.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"PLAINTEXT", "SASL_SSL"}, topicAddress.GetProtocols())
		assertions.Equal([]string{"sslCert+SCRAM"}, topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"localkafka.kafka-cluster:9092"}, topicAddress.GetBoostrapServers("PLAINTEXT"))
		assertions.Equal([]string{"localkafka.kafka-cluster:9094"}, topicAddress.GetBoostrapServers("SASL_SSL"))
		credentials := topicAddress.GetCredentials("sslCert+SCRAM")
		assertions.NotNil(credentials)
		assertions.Equal("alice", credentials.Username)
		assertions.Equal("alice-secret", credentials.Password)
		assertions.Equal("test-clientCert", credentials.ClientCert)
		assertions.Equal("test-clientKey", credentials.ClientKey)
	})
}

func Test_SSL(t *testing.T) {
	assertions := require.New(t)
	testResponse(t, "test/resources/SSL.json", func(topicAddress model.TopicAddress) {
		assertions.Equal("maas.test-namespace.test.topic", topicAddress.TopicName)
		assertions.Equal([]string{"SSL"}, topicAddress.GetProtocols())
		assertions.Nil(topicAddress.GetCredTypes())
		assertions.Equal(1, topicAddress.NumPartitions)
		assertions.Equal([]string{"localkafka.kafka-cluster:9094"}, topicAddress.GetBoostrapServers("SSL"))
		assertions.Equal("test-caCert", topicAddress.CACert)
	})
}

func Test_WatchCreateTopic(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	// waiting group to imitate processing on server side until additional watch requests are made
	testName1 := "watch-test-1"
	testName2 := "watch-test-2"
	waitWg := sync.WaitGroup{}
	waitWg.Add(1)
	triggerWg := sync.WaitGroup{}
	triggerWg.Add(1)
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v2/kafka/topic/watch-create" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var requestBody []classifier.Keys
			err = json.Unmarshal(reqBody, &requestBody)
			assertions.Nil(err)
			if len(requestBody) == 1 && requestBody[0][classifier.Name] == testName1 {
				triggerWg.Done()
				waitWg.Wait()
			}
			var responseBody []internal.TopicResponse
			for _, keys := range requestBody {
				responseBody = append(responseBody, internal.TopicResponse{
					Classifier: keys,
					Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
					Name:       keys[classifier.Name],
					Namespace:  keys[classifier.Namespace],
				})
			}
			bytes, err := json.Marshal(responseBody)
			assertions.Nil(err)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		}
	})
	defer ts.Close()
	client := NewClient(testNamespace, ts.URL, ts.URL, resty.New(), &websocket.Dialer{}, testAuthSupplier())
	var testWg1 sync.WaitGroup
	testWg1.Add(1)
	err := client.WatchTopicCreate(ctx, classifier.New(testName1), func(topicAddress model.TopicAddress) {
		assertions.Equal(testName1, topicAddress.Classifier[classifier.Name])
		testWg1.Done()
	})
	assertions.Nil(err)

	assertions.True(waitWithTimeout(&triggerWg, timeout))

	var testWg2 sync.WaitGroup
	testWg2.Add(1)
	err = client.WatchTopicCreate(ctx, classifier.New(testName2), func(topicAddress model.TopicAddress) {
		assertions.Equal(testName2, topicAddress.Classifier[classifier.Name])
		testWg2.Done()
	})
	assertions.Nil(err)
	// release lock from first request after second request was done
	// when we cancel first request on our side, we anticipate server closes canceled request
	waitWg.Done()

	assertions.True(waitWithTimeout(&testWg1, timeout))
	assertions.True(waitWithTimeout(&testWg2, timeout))
}

func Test_WatchCreateTopicsAsync(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v2/kafka/topic/watch-create" {
			time.Sleep(1 * time.Second) // sleep to test go-resty ctx.Cancel(), request must be cancelled immediately
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var requestBody []classifier.Keys
			err = json.Unmarshal(reqBody, &requestBody)
			assertions.Nil(err)
			var responseBody []internal.TopicResponse
			for _, keys := range requestBody {
				responseBody = append(responseBody, internal.TopicResponse{
					Classifier: keys,
					Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
					Name:       keys[classifier.Name],
					Namespace:  keys[classifier.Namespace],
				})
			}
			bytes, err := json.Marshal(responseBody)
			assertions.Nil(err)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		}
	})
	defer ts.Close()
	client := NewClient(testNamespace, ts.URL, ts.URL, resty.New(), &websocket.Dialer{}, testAuthSupplier())

	number := 100
	wgs := make(map[string]*sync.WaitGroup)
	errChan := make(chan error)
	for i := 0; i < number; i++ {
		wg := sync.WaitGroup{}
		wg.Add(1)
		name := strconv.Itoa(i)
		wgs[name] = &wg
		go func(name string) {
			errChan <- client.WatchTopicCreate(ctx, classifier.New(name), func(topicAddress model.TopicAddress) {
				assertions.Equal(name, topicAddress.Classifier[classifier.Name])
				wgs[name].Done()
			})
		}(name)
	}
	// verify all WatchTopicCreate invocations were successful
	for i := 0; i < number; i++ {
		err := <-errChan
		assertions.Nil(err)
	}
	for i := 0; i < number; i++ {
		assertions.True(waitWithTimeout(wgs[strconv.Itoa(i)], timeout))
	}
}

func Test_WatchTenantTopics(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	tenant1 := watch.Tenant{
		ExternalId: "1",
		Status:     watch.StatusActive,
		Name:       "tenant-1",
		Namespace:  testNamespace,
	}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1}}

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)
	topicAddress := internal.TopicResponse{
		Classifier: classifierKeys,
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       classifierKeys[classifier.Name],
		Namespace:  classifierKeys[classifier.Namespace],
	}

	var stompDestination, stompSubscriptionId string
	var wsConn *websocket.Conn

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var keys classifier.Keys
			err = json.Unmarshal(reqBody, &keys)
			assertions.Nil(err)

			bytes, err := json.Marshal(topicAddress)
			assertions.Nil(err)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {
			var err error
			wsConn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ = f3.Contains("destination")
			stompSubscriptionId, _ = f3.Contains("id")

			assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))
		}

	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	err := client.WatchTenantTopics(watchCtx1, classifierKeys, func(topicAddresses []model.TopicAddress) {
		assertions.Equal(1, len(topicAddresses))
		wg1.Done()
	})
	assertions.NoError(err)

	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	watchCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	err = client.WatchTenantTopics(watchCtx2, classifierKeys, func(topicAddresses []model.TopicAddress) {
		assertions.Equal(1, len(topicAddresses))
		wg2.Done()
	})
	assertions.NoError(err)

	assertions.True(waitWithTimeout(wg1, timeout))
	assertions.True(waitWithTimeout(wg2, timeout))
}

func Test_WatchTenantTopicNotFound(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	tenant1 := watch.Tenant{
		ExternalId: "1",
		Status:     watch.StatusActive,
		Name:       "tenant-1",
		Namespace:  testNamespace,
	}
	tenant2 := watch.Tenant{
		ExternalId: "2",
		Status:     watch.StatusActive,
		Name:       "tenant-2",
		Namespace:  testNamespace,
	}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1, tenant2}}

	topicAddress1 := internal.TopicResponse{
		Classifier: classifier.New("test").WithNamespace(testNamespace).WithTenantId(tenant1.ExternalId),
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       tenant1.Name,
		Namespace:  testNamespace,
	}
	topicAddress2 := internal.TopicResponse{
		Classifier: classifier.New("test").WithNamespace(testNamespace).WithTenantId(tenant2.ExternalId),
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       tenant2.Name,
		Namespace:  testNamespace,
	}

	var stompDestination, stompSubscriptionId string
	var wsConn *websocket.Conn

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var keys classifier.Keys
			err = json.Unmarshal(reqBody, &keys)
			assertions.Nil(err)
			if keys[classifier.TenantId] == topicAddress1.Classifier[classifier.TenantId] {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(""))
			} else if keys[classifier.TenantId] == topicAddress2.Classifier[classifier.TenantId] {
				bytes, err := json.Marshal(topicAddress2)
				assertions.Nil(err)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(bytes)
			}
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {
			var err error
			wsConn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ = f3.Contains("destination")
			stompSubscriptionId, _ = f3.Contains("id")

			assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))
		}
	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	err := client.WatchTenantTopics(watchCtx1, classifier.New("test").WithNamespace(testNamespace), func(topicAddresses []model.TopicAddress) {
		assertions.Equal(1, len(topicAddresses))
		assertions.Equal(tenant2.ExternalId, topicAddresses[0].Classifier[classifier.TenantId])
		wg1.Done()
	})
	assertions.NoError(err)

	assertions.True(waitWithTimeout(wg1, timeout))
}

func Test_WatchTenantTopicsWithRetry(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	tenant1 := watch.Tenant{
		ExternalId: "1",
		Status:     watch.StatusActive,
		Name:       "tenant-1",
		Namespace:  testNamespace,
	}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1}}

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)
	topicAddress := internal.TopicResponse{
		Classifier: classifierKeys,
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       classifierKeys[classifier.Name],
		Namespace:  classifierKeys[classifier.Namespace],
	}

	var stompDestination, stompSubscriptionId string
	var wsConn *websocket.Conn

	util.DefaultRetryAttempts = 5
	util.DefaultRetryInterval = 10 * time.Millisecond
	attempts := util.DefaultRetryAttempts - 1

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var keys classifier.Keys
			err = json.Unmarshal(reqBody, &keys)
			assertions.Nil(err)

			bytes, err := json.Marshal(topicAddress)
			assertions.Nil(err)
			if attempts > 0 {
				attempts--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {
			var err error
			wsConn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ = f3.Contains("destination")
			stompSubscriptionId, _ = f3.Contains("id")

			assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))
		}

	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	err := client.WatchTenantTopics(watchCtx1, classifierKeys, func(topicAddresses []model.TopicAddress) {
		assertions.Equal(1, len(topicAddresses))
		wg1.Done()
	})
	assertions.NoError(err)

	assertions.True(waitWithTimeout(wg1, timeout), "failed to wait for topics callback in watcher #1")

	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	watchCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	err = client.WatchTenantTopics(watchCtx2, classifierKeys, func(topicAddresses []model.TopicAddress) {
		assertions.Equal(1, len(topicAddresses))
		wg2.Done()
	})
	assertions.NoError(err)

	assertions.True(waitWithTimeout(wg2, timeout), "failed to wait for topics callback in watcher #2")
}

func Test_WatchTenantTopicsConnectionError(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)
	util.DefaultRetryAttempts = 5
	util.DefaultRetryInterval = 10 * time.Millisecond

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {

			// imitate TM connection error
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	err := client.WatchTenantKafkaTopics(watchCtx1, classifierKeys, func(topics []model.TopicAddress, err error) {
	})
	assertions.Error(err)
}

func Test_WatchTenantTopicsReConnectionAfterError(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	tenant1 := watch.Tenant{
		ExternalId: "1",
		Status:     watch.StatusActive,
		Name:       "tenant-1",
		Namespace:  testNamespace,
	}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1}}

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)
	topicAddress := internal.TopicResponse{
		Classifier: classifierKeys,
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       classifierKeys[classifier.Name],
		Namespace:  classifierKeys[classifier.Namespace],
	}

	var stompDestination, stompSubscriptionId string
	var wsConn *websocket.Conn

	util.DefaultRetryAttempts = 2
	util.DefaultRetryInterval = 10 * time.Millisecond
	connectAttempts := util.DefaultRetryAttempts - 1
	reconnectAttempts := 0

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var keys classifier.Keys
			err = json.Unmarshal(reqBody, &keys)
			assertions.Nil(err)

			bytes, err := json.Marshal(topicAddress)
			assertions.Nil(err)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {

			if connectAttempts > 0 {
				connectAttempts--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			var err error
			wsConn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ = f3.Contains("destination")
			stompSubscriptionId, _ = f3.Contains("id")

			assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))

			if reconnectAttempts == 0 {
				reconnectAttempts++
				wsConn.Close()
				return
			}
		}

	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	err := client.WatchTenantKafkaTopics(watchCtx1, classifierKeys, func(topics []model.TopicAddress, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		assertions.NoError(ctx.Err())
		assertions.Equal(1, len(topics))
		wg1.Done()
	})
	assertions.NoError(err)

	assertions.True(waitWithTimeout(wg1, timeout))

	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	watchCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	err2 := client.WatchTenantKafkaTopics(watchCtx2, classifierKeys, func(topics []model.TopicAddress, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		assertions.NoError(ctx.Err())
		assertions.Equal(1, len(topics))
		wg2.Done()
	})
	assertions.NoError(err2)

	assertions.True(waitWithTimeout(wg2, timeout))
}

func Test_WatchTenantTopicsClientNotifiedAboutConnectError(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	tenant1 := watch.Tenant{
		ExternalId: "1",
		Status:     watch.StatusActive,
		Name:       "tenant-1",
		Namespace:  testNamespace,
	}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1}}

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)
	topicAddress := internal.TopicResponse{
		Classifier: classifierKeys,
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       classifierKeys[classifier.Name],
		Namespace:  classifierKeys[classifier.Namespace],
	}

	var stompDestination, stompSubscriptionId string
	var wsConn *websocket.Conn

	util.DefaultRetryAttempts = 2
	util.DefaultRetryInterval = 10 * time.Millisecond

	abortWebSocketWg1 := &sync.WaitGroup{}
	abortWebSocketWg1.Add(1)

	// atomic: written by the test goroutine, read by the http handler goroutine
	var testPhase atomic.Int32

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var keys classifier.Keys
			err = json.Unmarshal(reqBody, &keys)
			assertions.Nil(err)

			bytes, err := json.Marshal(topicAddress)
			assertions.Nil(err)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {

			if testPhase.Load() != 0 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			var err error
			wsConn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ = f3.Contains("destination")
			stompSubscriptionId, _ = f3.Contains("id")

			assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))

			// close connection after command
			abortWebSocketWg1.Wait()

			wsConn.Close()
		}
	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	err := client.WatchTenantKafkaTopics(watchCtx1, classifierKeys, func(topics []model.TopicAddress, err error) {
		if testPhase.Load() == 0 {
			assertions.Equal(1, len(topics))
			wg1.Done()
		} else {
			// test that context was cancelled due to watcher failure to re-connect to TM
			assertions.NotNil(err)
			wg2.Done()
		}
	})
	assertions.NoError(err)

	assertions.True(waitWithTimeout(wg1, timeout))

	testPhase.Add(1)
	abortWebSocketWg1.Done()

	assertions.True(waitWithTimeout(wg2, timeout))
}

func Test_WatchTenantTopicsSubscribeTimeout(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)

	util.DefaultRetryAttempts = 1
	util.DefaultRetryInterval = 100 * time.Millisecond

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {

			// local: every retry is served on its own goroutine
			wsConn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)
		}
	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	go func() {
		err := client.WatchTenantKafkaTopics(watchCtx1, classifierKeys, func(topicAddresses []model.TopicAddress, err error) {
		})
		assertions.Error(err)
		wg1.Done()
	}()
	assertions.True(waitWithTimeout(wg1, timeout))
}

func Test_WatchTenantTopicsInfiniteLoop(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)
	tenant1 := watch.Tenant{ExternalId: "1", Status: watch.StatusActive, Name: "tenant-1", Namespace: testNamespace}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1}}

	topicAddress := internal.TopicResponse{
		Classifier: classifierKeys,
		Addresses:  map[string][]string{"PLAINTEXT": {"test.kafka.9092"}},
		Name:       classifierKeys[classifier.Name],
		Namespace:  classifierKeys[classifier.Namespace],
	}

	util.DefaultRetryAttempts = 1
	util.DefaultRetryInterval = 100 * time.Millisecond

	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	// atomic: each reconnect is served on its own goroutine
	var attempt atomic.Int32

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			reqBody, err := io.ReadAll(r.Body)
			assertions.Nil(err)
			var keys classifier.Keys
			err = json.Unmarshal(reqBody, &keys)
			assertions.Nil(err)

			bytes, err := json.Marshal(topicAddress)
			assertions.Nil(err)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes)
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {

			thisAttempt := attempt.Add(1)
			fmt.Printf("attempt=%d\n", thisAttempt)

			wsConn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ := f3.Contains("destination")
			stompSubscriptionId, _ := f3.Contains("id")

			// the first two connections hang, so the watcher has to reconnect twice
			if thisAttempt < 3 {
				select {}
			}
			if thisAttempt == 3 {
				assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))
			}
		}
	})

	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	go func() {
		defer func() {
			wg2.Done()
		}()
		for {
			watchCtx, cancelWatching := context.WithCancel(ctx)
			err := client.WatchTenantKafkaTopics(watchCtx, classifierKeys, func(topics []model.TopicAddress, err error) {
				if err != nil {
					cancelWatching()
					assertions.Error(watchCtx.Err())
					return
				}
				if len(topics) > 0 {
					wg1.Done()
					// terminate watcher so we can exit from infinite loop
					cancelWatching()
				}
			})
			switch attempt.Load() {
			case 2:
				assertions.Error(err)
			case 3:
				assertions.NoError(err)
			}
			<-watchCtx.Done()
			if attempt.Load() == 3 {
				assertions.True(waitWithTimeout(wg1, timeout))
				return
			}
		}
	}()
	assertions.True(waitWithTimeout(wg2, timeout))
}

func Test_WatchTenantTopicsMaasResponseErr(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	tenant1 := watch.Tenant{
		ExternalId: "1",
		Status:     watch.StatusActive,
		Name:       "tenant-1",
		Namespace:  testNamespace,
	}
	tenantEvent1 := &watch.TenantWatchEvent{Type: watch.SUBSCRIBED, Tenants: []watch.Tenant{tenant1}}

	classifierKeys := classifier.New("test").WithNamespace(testNamespace)

	var stompDestination, stompSubscriptionId string
	var wsConn *websocket.Conn

	util.DefaultRetryAttempts = 1
	util.DefaultRetryInterval = 100 * time.Millisecond

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			w.WriteHeader(http.StatusInternalServerError)
		} else if r.Method == resty.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/api/v4/tenant-manager/watch") &&
			r.Header.Get("Authorization") == "Bearer " + testTokenValue {

			var err error
			wsConn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
			assertions.NoError(err)

			f1, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("CONNECT", f1.Command)

			f2 := goStompWs.CreateFrame("CONNECTED", []string{})
			assertions.NoError(writeStompMsg(wsConn, f2))

			f3, err := readStompMsg(wsConn)
			assertions.NoError(err)
			assertions.Equal("SUBSCRIBE", f3.Command)

			stompDestination, _ = f3.Contains("destination")
			stompSubscriptionId, _ = f3.Contains("id")

			assertions.NoError(sendTenantEvent(wsConn, stompDestination, stompSubscriptionId, tenantEvent1))
		}
	})
	defer ts.Close()
	tmUrl, _ := url.Parse(ts.URL)
	tmUrl.Scheme = "ws"
	client := NewClient(testNamespace, ts.URL, tmUrl.String(), resty.New(), &websocket.Dialer{}, testAuthSupplier())
	watchCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		err := client.WatchTenantKafkaTopics(watchCtx1, classifierKeys, func(topicAddresses []model.TopicAddress, err error) {
			assertions.Error(err)
			wg2.Done()
		})
		assertions.NoError(err)
		wg1.Done()
	}()
	assertions.True(waitWithTimeout(wg1, timeout))
	assertions.True(waitWithTimeout(wg2, timeout))
}

func sendTenantEvent(c *websocket.Conn, stompDestination, stompSubscriptionId string, event *watch.TenantWatchEvent) error {
	f4 := goStompWs.CreateFrame("MESSAGE", []string{
		"destination:" + stompDestination,
		"message-id:1",
		"subscription:" + stompSubscriptionId},
	)
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	f4.Body = string(body)
	return writeStompMsg(c, f4)
}

func readStompMsg(c *websocket.Conn) (*goStompWs.Frame, error) {
	_, data, err := c.ReadMessage()
	if err != nil {
		return nil, err
	}
	data = append([]byte("a"), data...)
	return goStompWs.ReadFrame(data), nil
}

func writeStompMsg(c *websocket.Conn, f *goStompWs.Frame) error {
	stompMsg := append([]byte{'a'}, f.Bytes()...)
	return c.WriteMessage(websocket.TextMessage, stompMsg)
}

func testResponse(t *testing.T, resourceFile string, assertFunc func(topicAddress model.TopicAddress)) {
	assertions := require.New(t)
	ctx := context.Background()
	resource, err := os.ReadFile(resourceFile)
	assertions.Nil(err)
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost {
			if strings.HasPrefix(r.URL.Path, "/api/v1/kafka/topic") {
				onTopicExistsQueryParam := r.URL.Query().Get(internal.OnTopicExistsQueryParam)
				assertions.NotEmpty(onTopicExistsQueryParam)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(resource)
			}
		}
	})
	defer ts.Close()
	client := NewClient(testNamespace, ts.URL, ts.URL, resty.New(), &websocket.Dialer{}, testAuthSupplier())
	options := model.TopicCreateOptions{
		Name:          "test-topic",
		NumPartitions: 2,
		OnTopicExists: model.OnTopicExistsMerge,
	}
	topic, err := client.GetOrCreateTopic(ctx, classifier.New("test"), options)
	assertions.Nil(err)
	assertions.NotNil(topic)
	assertFunc(*topic)
}

func createTestServer(fn func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(fn))
}

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return true // completed normally
	case <-time.After(timeout):
		return false // timed out
	}
}

func testAuthSupplier() func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		return testTokenValue, nil
	}
}
