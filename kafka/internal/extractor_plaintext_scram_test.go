package internal

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/assert"
)

func TestPlaintextScramExtractor_Extract_Success(t *testing.T) {
	configVal1 := "value1"
	configVal2 := "value2"
	topic := model.TopicAddress{
		Classifier:    classifier.Keys{classifier.Name: "testName"},
		TopicName:     "test-topic",
		NumPartitions: 3,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localhost:9092"},
		},
		Credentials: map[string]model.TopicUserCredentials{
			"SCRAM": {
				Username: "user",
				Password: "pass",
			},
		},
		CACert:  "test-cert",
		Configs: map[string]*string{"config1": &configVal1, "config2": &configVal2},
	}
	extractor := &PlaintextScramExtractor{}
	props := extractor.Extract(topic)
	assert.NotNil(t, props)
	assert.Equal(t, "test-topic", props.TopicName)
	assert.Equal(t, 3, props.NumPartitions)
	assert.Equal(t, "PLAINTEXT", props.Protocol)
	assert.Equal(t, []string{"localhost:9092"}, props.BootstrapServers)
	assert.Equal(t, "SCRAM-SHA-512", props.SaslMechanism)
	assert.Equal(t, "user", props.Username)
	assert.Equal(t, "pass", props.Password)
}

func TestPlaintextScramExtractor_Extract_NoBootstrap(t *testing.T) {
	topic := model.TopicAddress{
		BoostrapServers: map[string][]string{},
		Credentials:     map[string]model.TopicUserCredentials{},
	}
	extractor := &PlaintextScramExtractor{}
	props := extractor.Extract(topic)
	assert.Nil(t, props)
}

func TestPlaintextScramExtractor_Extract_NoCredentials(t *testing.T) {
	topic := model.TopicAddress{
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localhost:9092"},
		},
		Credentials: map[string]model.TopicUserCredentials{},
	}
	extractor := &PlaintextScramExtractor{}
	props := extractor.Extract(topic)
	assert.Nil(t, props)
}

func TestPlaintextScramExtractor_Priority(t *testing.T) {
	extractor := &PlaintextScramExtractor{}
	assert.Equal(t, 15, extractor.Priority())
}
