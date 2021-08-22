package beeorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	validatedRegistry, def, err := registry.Validate()
	assert.Nil(t, err)
	defer def()
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()
	testLogger := &testLogHandler{}
	engine.RegisterQueryLogger(testLogger, false, true, false)

	l := engine.GetRedis().GetLocker()
	lock, has := l.Obtain("test_key", time.Second, 0)
	assert.True(t, has)
	assert.NotNil(t, lock)
	has = lock.Refresh(time.Second)
	assert.True(t, has)

	_, has = l.Obtain("test_key", time.Second, time.Millisecond)
	assert.False(t, has)

	left := lock.TTL()
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())

	lock.Release()
	lock.Release()
	has = lock.Refresh(time.Second)
	assert.False(t, has)
}
