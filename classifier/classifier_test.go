package classifier

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	k := New("foo")
	assert.Equal(t, Keys{Name: "foo"}, k)
}

func TestWithNamespace(t *testing.T) {
	k := New("foo").WithNamespace("ns")
	assert.Equal(t, "ns", k[Namespace])
}

func TestWithTenantId(t *testing.T) {
	k := New("foo").WithTenantId("tid")
	assert.Equal(t, "tid", k[TenantId])
}

func TestWithParam(t *testing.T) {
	k := New("foo").WithParam("custom", "bar")
	assert.Equal(t, "bar", k["custom"])
}

func TestAsString(t *testing.T) {
	k := New("foo").WithNamespace("ns").WithTenantId("tid").WithParam("custom", "bar")
	s := k.AsString()
	var m map[string]string
	err := json.Unmarshal([]byte(s), &m)
	assert.NoError(t, err)
	assert.Equal(t, "foo", m[Name])
	assert.Equal(t, "ns", m[Namespace])
	assert.Equal(t, "tid", m[TenantId])
	assert.Equal(t, "bar", m["custom"])
}
