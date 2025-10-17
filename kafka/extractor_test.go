package kafka

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/assert"
)

type mockExtractor struct {
	props    *model.TopicConnectionProperties
	priority int
}

func (m *mockExtractor) Extract(_ model.TopicAddress) *model.TopicConnectionProperties {
	return m.props
}
func (m *mockExtractor) Priority() int {
	return m.priority
}

func TestExtract_ReturnsFirstNonNil(t *testing.T) {
	topic := model.TopicAddress{TopicName: "t1"}
	ex1 := &mockExtractor{props: nil, priority: 20}
	ex2 := &mockExtractor{props: &model.TopicConnectionProperties{TopicName: "t1"}, priority: 10}
	oldExtractors := defaultExtractors
	defaultExtractors = []ConnectionPropertiesExtractor{ex1, ex2}
	defer func() { defaultExtractors = oldExtractors }()

	props, err := Extract(topic)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	assert.Equal(t, "t1", props.TopicName)
}

func TestExtract_AllNil_ReturnsError(t *testing.T) {
	topic := model.TopicAddress{TopicName: "t2"}
	ex1 := &mockExtractor{props: nil, priority: 5}
	ex2 := &mockExtractor{props: nil, priority: 1}
	oldExtractors := defaultExtractors
	defaultExtractors = []ConnectionPropertiesExtractor{ex1, ex2}
	defer func() { defaultExtractors = oldExtractors }()

	props, err := Extract(topic)
	assert.Nil(t, props)
	assert.Error(t, err)
}

func TestRegisterExtractor_AppendsExtractor(t *testing.T) {
	ex := &mockExtractor{props: nil, priority: 99}
	oldExtractors := defaultExtractors
	defaultExtractors = []ConnectionPropertiesExtractor{}
	defer func() { defaultExtractors = oldExtractors }()

	RegisterExtractor(ex)
	assert.Equal(t, 1, len(defaultExtractors))
	assert.Equal(t, ex, defaultExtractors[0])
}

func TestGetSortedExtractors_SortsAndMapsByPriority(t *testing.T) {
	ex1 := &mockExtractor{priority: 2}
	ex2 := &mockExtractor{priority: 5}
	ex3 := &mockExtractor{priority: 1}
	extractors := []ConnectionPropertiesExtractor{ex1, ex2, ex3}
	priorities, exMap := getSortedExtractors(extractors)
	assert.Equal(t, []int{5, 2, 1}, priorities)
	assert.Equal(t, ex1, exMap[2])
	assert.Equal(t, ex2, exMap[5])
	assert.Equal(t, ex3, exMap[1])
}

func TestGetSortedExtractors_PanicsOnDuplicatePriority(t *testing.T) {
	ex1 := &mockExtractor{priority: 2}
	ex2 := &mockExtractor{priority: 2}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on duplicate priority")
		}
	}()
	getSortedExtractors([]ConnectionPropertiesExtractor{ex1, ex2})
}
