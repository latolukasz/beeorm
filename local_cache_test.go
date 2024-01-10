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
	orm := validatedRegistry.NewORM(context.Background())
	testLogger := &MockLogHandler{}
	orm.RegisterQueryLogger(testLogger, false, false, true)
	testQueryLog := &MockLogHandler{}
	orm.RegisterQueryLogger(testQueryLog, false, false, true)

	lc := orm.Engine().LocalCache(DefaultPoolCode)
	assert.Equal(t, DefaultPoolCode, lc.GetConfig().GetCode())
	assert.Equal(t, 0, lc.GetConfig().GetLimit())

	val, has := lc.Get(orm, "test_get")
	assert.False(t, has)
	assert.Nil(t, val)

	lc.Set(orm, "test_get", "hello")
	val, has = lc.Get(orm, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)

	lcLimit := orm.Engine().LocalCache("with_limit")
	assert.Equal(t, "with_limit", lcLimit.GetConfig().GetCode())
	assert.Equal(t, 3, lcLimit.GetConfig().GetLimit())
	lcLimit.Set(orm, "1", "1")
	lcLimit.Set(orm, "2", "2")
	lcLimit.Set(orm, "3", "3")
	assert.Equal(t, uint64(3), lcLimit.GetUsage()[0].Used)
	assert.Equal(t, uint64(0), lcLimit.GetUsage()[0].Evictions)
	lcLimit.Set(orm, "1", "1")
	assert.Equal(t, uint64(3), lcLimit.GetUsage()[0].Used)
	assert.Equal(t, uint64(0), lcLimit.GetUsage()[0].Evictions)
	lcLimit.Set(orm, "4", "4")
	assert.Equal(t, uint64(3), lcLimit.GetUsage()[0].Used)
	assert.Equal(t, uint64(1), lcLimit.GetUsage()[0].Evictions)
	_, found := lcLimit.Get(orm, "4")
	assert.True(t, found)

	orm = validatedRegistry.NewORM(context.Background())
	orm.RegisterQueryLogger(testLogger, false, false, true)
	orm.RegisterQueryLogger(testQueryLog, false, false, true)
	lc = orm.Engine().LocalCache(DefaultPoolCode)
	val, has = lc.Get(orm, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
}
