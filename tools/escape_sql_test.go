package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeSQLParam(t *testing.T) {
	assert.Equal(t, "'\\\\ss\\\\'", EscapeSQLParam("\\ss\\"))
}
