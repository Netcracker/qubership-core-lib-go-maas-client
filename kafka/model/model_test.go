package model

import (
	"reflect"
	"sort"
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/stretchr/testify/assert"
)

// let's assume that configs section didn't received from maas-server
func TestTopicAddress_GetConfig(t *testing.T) {
	retentionValue := "50000"
	ta := TopicAddress{Configs: map[string]*string{"retention.ms": &retentionValue}}
	assert.Equal(t, &retentionValue, ta.GetConfig("retention.ms"))
}

// let's assume that configs section didn't received from maas-server
func TestTopicAddress_GetConfig_Empty(t *testing.T) {
	ta := TopicAddress{}
	assert.Nil(t, ta.GetConfig("absent"))
}

func TestTopicAddress_GetClassifier(t *testing.T) {
	keys := classifier.Keys{classifier.Name: "foo"}
	ta := TopicAddress{Classifier: keys}
	assert.Equal(t, keys, ta.GetClassifier())
}

func TestTopicAddress_GetBoostrapServers(t *testing.T) {
	ta := TopicAddress{
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"a", "b"},
			"SSL":       {"c"},
		},
	}
	assert.Equal(t, []string{"a", "b"}, ta.GetBoostrapServers("PLAINTEXT"))
	assert.Equal(t, []string{"c"}, ta.GetBoostrapServers("SSL"))
	assert.Nil(t, ta.GetBoostrapServers("SASL"))
}

func TestTopicAddress_GetProtocols(t *testing.T) {
	ta := TopicAddress{
		BoostrapServers: map[string][]string{
			"SSL":       {"c"},
			"PLAINTEXT": {"a", "b"},
		},
	}
	protocols := ta.GetProtocols()
	expected := []string{"PLAINTEXT", "SSL"}
	sort.Strings(expected)
	assert.True(t, reflect.DeepEqual(expected, protocols))
}

func TestTopicAddress_GetCredTypes(t *testing.T) {
	ta := TopicAddress{
		Credentials: map[string]TopicUserCredentials{
			"SCRAM": {Username: "u"},
			"PLAIN": {Username: "v"},
		},
	}
	credTypes := ta.GetCredTypes()
	expected := []string{"PLAIN", "SCRAM"}
	sort.Strings(expected)
	assert.True(t, reflect.DeepEqual(expected, credTypes))
}

func TestTopicAddress_GetCredentials(t *testing.T) {
	creds := map[string]TopicUserCredentials{
		"SCRAM": {Username: "u", Password: "p"},
	}
	ta := TopicAddress{Credentials: creds}
	got := ta.GetCredentials("SCRAM")
	assert.NotNil(t, got)
	assert.Equal(t, "u", got.Username)
	assert.Equal(t, "p", got.Password)
	assert.Nil(t, ta.GetCredentials("PLAIN"))
}
