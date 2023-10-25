package beeorm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalCache(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterLocalCache(DefaultPoolCode, 0)
	registry.RegisterLocalCache("with_limit", 3)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	c := validatedRegistry.NewContext(context.Background())
	testLogger := &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, false, true)
	testQueryLog := &MockLogHandler{}
	c.RegisterQueryLogger(testQueryLog, false, false, true)

	lc := c.Engine().LocalCache(DefaultPoolCode)
	assert.Equal(t, DefaultPoolCode, lc.GetCode())

	val, has := lc.Get(c, "test_get")
	assert.False(t, has)
	assert.Nil(t, val)

	lc.Set(c, "test_get", "hello")
	val, has = lc.Get(c, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)

	lcLimit := c.Engine().LocalCache("with_limit")
	lcLimit.Set(c, "1", "1")
	lcLimit.Set(c, "2", "2")
	lcLimit.Set(c, "3", "3")
	assert.Equal(t, 3, lcLimit.GetObjectsCount())
	lcLimit.Set(c, "1", "1")
	assert.Equal(t, 3, lcLimit.GetObjectsCount())
	lcLimit.Set(c, "4", "4")
	assert.Equal(t, 3, lcLimit.GetObjectsCount())
	_, found := lcLimit.Get(c, "4")
	assert.True(t, found)

	c = validatedRegistry.NewContext(context.Background())
	c.RegisterQueryLogger(testLogger, false, false, true)
	c.RegisterQueryLogger(testQueryLog, false, false, true)
	lc = c.Engine().LocalCache(DefaultPoolCode)
	val, has = lc.Get(c, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
}
