package beeorm

import (
	"context"
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
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	c := validatedRegistry.NewContext(context.Background())
	c.Engine().Redis(DefaultPoolCode).FlushDB(c)
	testLogger := &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, true, false)

	l := c.Engine().Redis(DefaultPoolCode).GetLocker()
	lock, has := l.Obtain(c, "test_key", time.Second, 0)
	assert.True(t, has)
	assert.NotNil(t, lock)
	has = lock.Refresh(c, time.Second)
	assert.True(t, has)

	_, has = l.Obtain(c, "test_key", time.Second, time.Millisecond*100)
	assert.False(t, has)

	left := lock.TTL(c)
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())

	_, has = l.Obtain(c, "test_key", time.Second*10, time.Second*10)
	assert.True(t, has)

	lock.Release(c)
	lock.Release(c)
	has = lock.Refresh(c, time.Second)
	assert.False(t, has)

	assert.PanicsWithError(t, "ttl must be higher than zero", func() {
		_, _ = l.Obtain(c, "test_key", 0, time.Millisecond)
	})
	assert.PanicsWithError(t, "waitTimeout can't be higher than ttl", func() {
		_, _ = l.Obtain(c, "test_key", time.Second, time.Second*2)
	})
}
