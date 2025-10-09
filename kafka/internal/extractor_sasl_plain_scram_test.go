package internal

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/assert"
)

func TestSaslPlaintextScramExtractor_Extract_Success(t *testing.T) {
	configVal := "val"
	topic := model.TopicAddress{
		Classifier:    classifier.Keys{classifier.Name: "testName"},
		TopicName:     "test-topic",
		NumPartitions: 2,
		BoostrapServers: map[string][]string{
			"SASL_PLAINTEXT": {"localhost:9093"},
		},
		Credentials: map[string]model.TopicUserCredentials{
			"SCRAM": {
				Username: "sasluser",
				Password: "saslpass",
			},
		},
		CACert:  "ca-cert",
		Configs: map[string]*string{"foo": &configVal},
	}
	extractor := &SaslPlaintextScramExtractor{}
	props := extractor.Extract(topic)
	assert.NotNil(t, props)
	assert.Equal(t, "test-topic", props.TopicName)
	assert.Equal(t, 2, props.NumPartitions)
	assert.Equal(t, "SASL_PLAINTEXT", props.Protocol)
	assert.Equal(t, []string{"localhost:9093"}, props.BootstrapServers)
	assert.Equal(t, "SCRAM-SHA-512", props.SaslMechanism)
	assert.Equal(t, "sasluser", props.Username)
	assert.Equal(t, "saslpass", props.Password)
}

func TestSaslPlaintextScramExtractor_Extract_NoBootstrap(t *testing.T) {
	topic := model.TopicAddress{
		BoostrapServers: map[string][]string{},
		Credentials:     map[string]model.TopicUserCredentials{},
	}
	extractor := &SaslPlaintextScramExtractor{}
	props := extractor.Extract(topic)
	assert.Nil(t, props)
}

func TestSaslPlaintextScramExtractor_Extract_NoCredentials(t *testing.T) {
	topic := model.TopicAddress{
		BoostrapServers: map[string][]string{
			"SASL_PLAINTEXT": {"localhost:9093"},
		},
		Credentials: map[string]model.TopicUserCredentials{},
	}
	extractor := &SaslPlaintextScramExtractor{}
	props := extractor.Extract(topic)
	assert.Nil(t, props)
}

func TestSaslPlaintextScramExtractor_Priority(t *testing.T) {
	extractor := &SaslPlaintextScramExtractor{}
	assert.Equal(t, 10, extractor.Priority())
}
