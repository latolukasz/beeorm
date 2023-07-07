package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalCache(t *testing.T) {
	registry := &Registry{}
	registry.RegisterLocalCache(100)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	testLogger := &MockLogHandler{}
	engine.RegisterQueryLogger(testLogger, false, false, true)
	testQueryLog := &MockLogHandler{}
	engine.RegisterQueryLogger(testQueryLog, false, false, true)

	c := engine.GetLocalCache()
	assert.Equal(t, "default", c.GetPoolConfig().GetCode())
	assert.Equal(t, 100, c.GetPoolConfig().GetLimit())
	val := c.GetSet("test_get_set", 10, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Logs, 2)
	val = c.GetSet("test_get_set", 10, func() interface{} {
		return "hello2"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Logs, 3)

	val, has := c.Get("test_get")
	assert.False(t, has)
	assert.Nil(t, val)

	c.Set("test_get", "hello")
	val, has = c.Get("test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)

	engine = validatedRegistry.CreateEngine()
	engine.RegisterQueryLogger(testLogger, false, false, true)
	engine.RegisterQueryLogger(testQueryLog, false, false, true)
	c = engine.GetLocalCache()
	val, has = c.Get("test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
}
