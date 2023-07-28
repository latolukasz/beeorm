package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
)

func TestRedisPipeline(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", "", 15)
	registry.RegisterRedisStream("test-stream", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream", "test-group")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	c := validatedRegistry.NewContext(context.Background())
	r := c.Engine().GetRedis()
	r.FlushDB(c)
	pipeLine := r.PipeLine()

	r.Set(c, "a", "A", 10*time.Second)
	r.Set(c, "c", "C", 10*time.Second)
	testLogger := &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, true, false)
	c1 := pipeLine.Get(c, "a")
	c2 := pipeLine.Get(c, "b")
	c3 := pipeLine.Get(c, "c")
	pipeLine.Exec(c)
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
	c4 := pipeLine.XAdd(c, "test-stream", []string{"key", "a", "key2", "b"})
	c5 := pipeLine.XAdd(c, "test-stream", []string{"key", "c", "key2", "d"})
	_ = pipeLine.XAdd(c, "a", []string{"key", "c", "key2", "d"})
	c7 := pipeLine.XAdd(c, "test-stream", []string{"key", "e", "key2", "f"})
	assert.Panics(t, func() {
		pipeLine.Exec(c)
	})
	val = c4.Result()
	assert.NotEmpty(t, val)
	val = c5.Result()
	assert.NotEmpty(t, val)
	val = c7.Result()
	assert.NotEmpty(t, val)

	assert.Equal(t, int64(3), r.XLen(c, "test-stream"))
	events := r.XRead(c, &redis.XReadArgs{Count: 10, Streams: []string{"test-stream", "0"}, Block: time.Millisecond * 500})
	assert.Len(t, events, 1)
	assert.Len(t, events[0].Messages, 3)

	pipeLine = r.PipeLine()
	pipeLine.Set(c, "test_set", "test_value", time.Minute)
	pipeLine.Exec(c)
	returnedVal, has := c.Engine().GetRedis().Get(c, "test_set")
	assert.True(t, has)
	assert.Equal(t, "test_value", returnedVal)

	pipeLine = r.PipeLine()
	boolResult := pipeLine.Expire(c, "test_set", time.Hour)
	pipeLine.Exec(c)
	assert.True(t, boolResult.Result())

	pipeLine = r.PipeLine()
	pipeLine.Del(c, "test_set")
	pipeLine.Exec(c)
	_, has = c.Engine().GetRedis().Get(c, "test_set")
	assert.False(t, has)

	pipeLine = r.PipeLine()
	pipeLine.HSet(c, "test_hset", "a", "b", "c", "d")
	pipeLine.Exec(c)
	hSetValues := c.Engine().GetRedis().HGetAll(c, "test_hset")
	assert.Len(t, hSetValues, 2)
	assert.Equal(t, "b", hSetValues["a"])
	assert.Equal(t, "d", hSetValues["c"])

	pipeLine = r.PipeLine()
	pipeLine.HDel(c, "test_hset", "c")
	pipeLine.Exec(c)
	hSetValues = c.Engine().GetRedis().HGetAll(c, "test_hset")
	assert.Len(t, hSetValues, 1)
	assert.Equal(t, "b", hSetValues["a"])

	pipeLine = r.PipeLine()
	intRes := pipeLine.HIncrBy(c, "test_inc", "a", 2)
	pipeLine.Exec(c)
	returnedVal, hasVal := c.Engine().GetRedis().HGet(c, "test_inc", "a")
	assert.True(t, hasVal)
	assert.Equal(t, "2", returnedVal)
	assert.Equal(t, int64(2), intRes.Result())
}
