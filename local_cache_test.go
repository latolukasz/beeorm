package beeorm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalCache(t *testing.T) {
	registry := &Registry{}
	registry.RegisterLocalCache(100)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	c := validatedRegistry.NewContext(context.Background())
	testLogger := &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, false, true)
	testQueryLog := &MockLogHandler{}
	c.RegisterQueryLogger(testQueryLog, false, false, true)

	lc := c.Engine().LocalCache(DefaultPoolCode)
	assert.Equal(t, DefaultPoolCode, lc.GetPoolConfig().GetCode())
	assert.Equal(t, 100, lc.GetPoolConfig().GetLimit())

	val, has := lc.Get(c, "test_get")
	assert.False(t, has)
	assert.Nil(t, val)

	lc.Set(c, "test_get", "hello")
	val, has = lc.Get(c, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)

	c = validatedRegistry.NewContext(context.Background())
	c.RegisterQueryLogger(testLogger, false, false, true)
	c.RegisterQueryLogger(testQueryLog, false, false, true)
	lc = c.Engine().LocalCache(DefaultPoolCode)
	val, has = lc.Get(c, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
}
