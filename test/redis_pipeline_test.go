package test

import (
	"github.com/latolukasz/beeorm"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedisPipeline(t *testing.T) {
	registry := &beeorm.Registry{}
	registry.RegisterRedis("localhost:6382", "", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()
	pipeLine := r.PipeLine()

	r.Set("a", "A", 10*time.Second)
	r.Set("c", "C", 10*time.Second)
	testLogger := &MockLogHandler{}
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

	pipeLine = r.PipeLine()
	pipeLine.Set("test_set", "test_value", time.Minute)
	pipeLine.Exec()
	returnedVal, has := engine.GetRedis().Get("test_set")
	assert.True(t, has)
	assert.Equal(t, "test_value", returnedVal)

	pipeLine = r.PipeLine()
	boolResult := pipeLine.Expire("test_set", time.Hour)
	pipeLine.Exec()
	assert.True(t, boolResult.Result())

	pipeLine = r.PipeLine()
	pipeLine.Del("test_set")
	pipeLine.Exec()
	_, has = engine.GetRedis().Get("test_set")
	assert.False(t, has)

	pipeLine = r.PipeLine()
	pipeLine.HSet("test_hset", "a", "b", "c", "d")
	pipeLine.Exec()
	hSetValues := engine.GetRedis().HGetAll("test_hset")
	assert.Len(t, hSetValues, 2)
	assert.Equal(t, "b", hSetValues["a"])
	assert.Equal(t, "d", hSetValues["c"])

	pipeLine = r.PipeLine()
	pipeLine.HDel("test_hset", "c")
	pipeLine.Exec()
	hSetValues = engine.GetRedis().HGetAll("test_hset")
	assert.Len(t, hSetValues, 1)
	assert.Equal(t, "b", hSetValues["a"])

	pipeLine = r.PipeLine()
	intRes := pipeLine.HIncrBy("test_inc", "a", 2)
	pipeLine.Exec()
	returnedVal, hasVal := engine.GetRedis().HGet("test_inc", "a")
	assert.True(t, hasVal)
	assert.Equal(t, "2", returnedVal)
	assert.Equal(t, int64(2), intRes.Result())
}
