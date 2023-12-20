package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6385", 15, DefaultPoolCode, nil)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	orm := validatedRegistry.NewORM(context.Background())
	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	testLogger := &MockLogHandler{}
	orm.RegisterQueryLogger(testLogger, false, true, false)

	l := orm.Engine().Redis(DefaultPoolCode).GetLocker()
	lock, has := l.Obtain(orm, "test_key", time.Second, 0)
	assert.True(t, has)
	assert.NotNil(t, lock)
	has = lock.Refresh(orm, time.Second)
	assert.True(t, has)

	_, has = l.Obtain(orm, "test_key", time.Second, time.Millisecond*100)
	assert.False(t, has)

	left := lock.TTL(orm)
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())
	lock.Release(orm) // dragonfly-db fix

	_, has = l.Obtain(orm, "test_key", time.Second*10, time.Second*10)
	assert.True(t, has)

	lock.Release(orm)
	lock.Release(orm)
	has = lock.Refresh(orm, time.Second)
	assert.False(t, has)

	assert.PanicsWithError(t, "ttl must be higher than zero", func() {
		_, _ = l.Obtain(orm, "test_key", 0, time.Millisecond)
	})
	assert.PanicsWithError(t, "waitTimeout can't be higher than ttl", func() {
		_, _ = l.Obtain(orm, "test_key", time.Second, time.Second*2)
	})
}
