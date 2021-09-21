package beeorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	testLocker(t, "")
}

func TestLockerNamespace(t *testing.T) {
	testLocker(t, "test")
}

func testLocker(t *testing.T, namespace string) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", namespace, 15)
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

	assert.PanicsWithError(t, "ttl must be higher than zero", func() {
		_, _ = l.Obtain("test_key", 0, time.Millisecond)
	})
}
