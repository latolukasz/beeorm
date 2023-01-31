package test

import (
	"context"
	"testing"
	"time"

	"github.com/latolukasz/beeorm"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	testLocker(t, "")
}

func TestLockerNamespace(t *testing.T) {
	testLocker(t, "test")
}

func testLocker(t *testing.T, namespace string) {
	registry := &beeorm.Registry{}
	registry.RegisterRedis("localhost:6382", namespace, 15)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()
	testLogger := &MockLogHandler{}
	engine.RegisterQueryLogger(testLogger, false, true, false)

	l := engine.GetRedis().GetLocker()
	lock, has := l.Obtain(context.Background(), "test_key", time.Second, 0)
	assert.True(t, has)
	assert.NotNil(t, lock)
	has = lock.Refresh(context.Background())
	assert.True(t, has)

	_, has = l.Obtain(context.Background(), "test_key", time.Second, time.Millisecond*100)
	assert.False(t, has)

	left := lock.TTL()
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())

	_, has = l.Obtain(context.Background(), "test_key", time.Second, time.Second*10)
	assert.True(t, has)

	lock.Release()
	lock.Release()
	has = lock.Refresh(context.Background())
	assert.False(t, has)

	assert.PanicsWithError(t, "ttl must be higher than zero", func() {
		_, _ = l.Obtain(context.Background(), "test_key", 0, time.Millisecond)
	})
}
