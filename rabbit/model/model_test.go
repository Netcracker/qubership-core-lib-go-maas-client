package model

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVhost_Getters(t *testing.T) {
	vhost := Vhost{
		Cnn:             "amqp://rabbitmq.rabbitmq:2345",
		Username:        "scott",
		EncodedPassword: "plain:tiger",
	}

	assert.Equal(t, "amqp://rabbitmq.rabbitmq:2345", vhost.GetConnectionUri())
	assert.Equal(t, "scott", vhost.GetUsername())

	pwd, err := vhost.GetPassword()
	assert.NoError(t, err)
	assert.Equal(t, "tiger", pwd)
}

func TestVhost_GetPassword_Err(t *testing.T) {
	vhost := Vhost{
		EncodedPassword: "notimplemented:abc",
	}

	_, err := vhost.GetPassword()
	assert.Error(t, err)
}

func TestVhost_ToString_ObfuscatePassword(t *testing.T) {
	vhost := Vhost{
		Cnn:             "amqp://lala",
		Username:        "scott",
		EncodedPassword: "plain:tiger",
	}

	assert.Equal(t, "{Cnn:amqp://lala Username:scott ****}", fmt.Sprintf("%v", vhost))
}
