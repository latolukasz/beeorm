package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	ctx := context.Background()
	validatedRegistry, err := registry.Validate(ctx)
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
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
	lock.has = true
	lock.done = make(chan bool)
	lock.Release()
	_, has = l.Obtain("test_key", time.Second, time.Millisecond)
	assert.True(t, has)
	lock.has = true
	lock.done = make(chan bool)
	has = lock.Refresh(time.Second)
	assert.False(t, has)

	assert.PanicsWithError(t, "ttl must be higher than zero", func() {
		_, _ = l.Obtain("test_key", 0, time.Millisecond)
	})

	lock, has = l.Obtain("test_key_2", time.Millisecond*3, 0)
	assert.True(t, has)
	time.Sleep(time.Millisecond * 4)
	assert.Equal(t, time.Duration(0), lock.TTL())

	ctxCancel, cancel := context.WithCancel(engine.context)
	l2 := validatedRegistry.CreateEngine(ctxCancel).GetRedis().GetLocker()
	lock, has = l2.Obtain("test_key_3", time.Millisecond*3, 0)
	assert.True(t, has)
	cancel()
	time.Sleep(time.Millisecond)
	assert.PanicsWithError(t, "context canceled", func() {
		assert.Equal(t, time.Duration(0), lock.TTL())
	})

	registry = &Registry{}
	registry.RegisterRedis("localhost:6389", 15)
	validatedRegistry, err = registry.Validate(ctx)
	assert.NoError(t, err)
	engine = validatedRegistry.CreateEngine(ctx)
	testLogger = &testLogHandler{}
	engine.RegisterQueryLogger(testLogger, false, true, false)
	l = engine.GetRedis().GetLocker()
	assert.Panics(t, func() {
		_, _ = l.Obtain("test_key", time.Second, time.Millisecond)
	})
}
