package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeSQLParam(t *testing.T) {
	value := []byte{0, '\n', '\r', '\\', '\'', '"', '\032'}
	assert.Equal(t, "'\\0\\n\\r\\\\\\'\\\"\\Z'", EscapeSQLParam(string(value)))
	assert.Equal(t, "'test'", EscapeSQLParam("test"))
}
