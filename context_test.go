package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {
	c := PrepareTables(t, &Registry{}, 5, 6, "")
	source := c.Engine().Registry()
	assert.NotNil(t, source)
	assert.PanicsWithError(t, "unregistered mysql pool 'test'", func() {
		c.Engine().GetMySQL("test")
	})
	assert.PanicsWithError(t, "unregistered local cache pool 'test'", func() {
		c.Engine().GetLocalCache("test")
	})
	assert.PanicsWithError(t, "unregistered redis cache pool 'test'", func() {
		c.Engine().GetRedis("test")
	})
}
