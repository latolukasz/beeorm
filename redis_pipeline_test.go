package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestRedisPipeline(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	ctx := context.Background()
	validatedRegistry, err := registry.Validate(ctx)
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
	r := engine.GetRedis()
	r.FlushDB()
	pipeLine := r.PipeLine()

	r.Set("a", "A", 10)
	r.Set("c", "C", 10)
	testLogger := &testLogHandler{}
	engine.RegisterQueryLogger(testLogger, false, true, false)
	c1 := pipeLine.Get("a")
	c2 := pipeLine.Get("b")
	c3 := pipeLine.Get("c")
	pipeLine.Exec()
	assert.Len(t, testLogger.Logs, 1)
	val, has := c1.Result()
	assert.Equal(t, "A", val)
	assert.True(t, has)
	val, has = c2.Result()
	assert.Equal(t, "", val)
	assert.False(t, has)
	val, has = c3.Result()
	assert.Equal(t, "C", val)
	assert.True(t, has)

	pipeLine = r.PipeLine()
	c4 := pipeLine.XAdd("test-stream", []string{"key", "a", "key2", "b"})
	c5 := pipeLine.XAdd("test-stream", []string{"key", "c", "key2", "d"})
	_ = pipeLine.XAdd("a", []string{"key", "c", "key2", "d"})
	c7 := pipeLine.XAdd("test-stream", []string{"key", "e", "key2", "f"})
	assert.Panics(t, func() {
		pipeLine.Exec()
	})
	val = c4.Result()
	assert.NotEmpty(t, val)
	val = c5.Result()
	assert.NotEmpty(t, val)
	val = c7.Result()
	assert.NotEmpty(t, val)

	assert.Equal(t, int64(3), r.XLen("test-stream"))
	events := r.XRead(&redis.XReadArgs{Count: 10, Streams: []string{"test-stream", "0"}, Block: time.Millisecond * 500})
	assert.Len(t, events, 1)
	assert.Len(t, events[0].Messages, 3)
}
