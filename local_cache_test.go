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

	lc := c.Engine().GetLocalCache("")
	assert.Equal(t, "default", lc.GetPoolConfig().GetCode())
	assert.Equal(t, 100, lc.GetPoolConfig().GetLimit())
	val := lc.GetSet(c, "test_get_set", 10, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Logs, 2)
	val = lc.GetSet(c, "test_get_set", 10, func() interface{} {
		return "hello2"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Logs, 3)

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
	lc = c.Engine().GetLocalCache("")
	val, has = lc.Get(c, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
}
