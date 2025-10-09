package internal

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/require"
)

func Test_PlaintextExtractor_NoCreds_Extract(t *testing.T) {
	assertions := require.New(t)
	extractor := &PlaintextExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
		},
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal("", connectionProperties.SaslMechanism)
	assertions.Equal("", connectionProperties.Username)
	assertions.Equal("", connectionProperties.Password)
	assertions.Equal("PLAINTEXT", connectionProperties.Protocol)
	assertions.Equal("localkafka.kafka-cluster:9092", connectionProperties.BootstrapServers[0])
}

func Test_PlaintextExtractor_WithCreds_Extract(t *testing.T) {
	extractor := &PlaintextExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
		},
		Credentials: map[string]model.TopicUserCredentials{
			"plain": {
				Username: "user",
				Password: "pass",
			},
		},
	})
	require.NotNil(t, connectionProperties)
	require.Equal(t, "PLAIN", connectionProperties.SaslMechanism)
	require.Equal(t, "user", connectionProperties.Username)
	require.Equal(t, "pass", connectionProperties.Password)
	require.Equal(t, "PLAINTEXT", connectionProperties.Protocol)
}
