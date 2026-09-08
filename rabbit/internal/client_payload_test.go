package internal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/stretchr/testify/require"
)

// /api/v1/rabbit/vhost binds VHostRegistrationReqDto, so the classifier has to
// be wrapped. A bare classifier is rejected with 400.
func Test_GetOrCreateVhost_SendsTheClassifierWrapped(t *testing.T) {
	assertions := require.New(t)

	var body []byte
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"cnn":"amqp://127.0.0.1:5672/ns.test","username":"user","password":"plain:password"}`))
	})
	defer ts.Close()

	client := &CrudClient{MaasAgentUrl: ts.URL, Namespace: testNamespace, HttpClient: resty.New()}
	_, err := client.GetOrCreateVhost(context.Background(), classifier.New("test").WithNamespace(testNamespace))
	assertions.NoError(err)

	var request struct {
		Classifier map[string]string `json:"classifier"`
	}
	assertions.NoError(json.Unmarshal(body, &request))
	assertions.Equal(map[string]string{"name": "test", "namespace": testNamespace}, request.Classifier,
		"the classifier must travel under the classifier field, body was: %s", body)
}

// get-by-classifier is the neighbouring endpoint and takes the bare classifier.
func Test_GetVhost_SendsTheBareClassifier(t *testing.T) {
	assertions := require.New(t)

	var body []byte
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"vhost":{"cnn":"amqp://127.0.0.1:5672/ns.test","username":"user","password":"plain:password"}}`))
	})
	defer ts.Close()

	client := &CrudClient{MaasAgentUrl: ts.URL, Namespace: testNamespace, HttpClient: resty.New()}
	_, err := client.GetVhost(context.Background(), classifier.New("test").WithNamespace(testNamespace))
	assertions.NoError(err)

	var request map[string]string
	assertions.NoError(json.Unmarshal(body, &request))
	assertions.Equal(map[string]string{"name": "test", "namespace": testNamespace}, request,
		"the body must be the classifier itself, body was: %s", body)
}
