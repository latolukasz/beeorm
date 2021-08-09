package tools

import (
	"context"
	"testing"
	"time"

	orm "github.com/latolukasz/beeorm"
	"github.com/stretchr/testify/assert"
)

func TestRedisStreamsStatus(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6382", 11)
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	ctx := context.Background()
	validatedRegistry, def, err := registry.Validate(ctx)
	assert.NoError(t, err)
	defer def()
	engine := validatedRegistry.CreateEngine(ctx)
	r := engine.GetRedis()
	r.FlushDB()

	stats := GetRedisStreamsStatistics(engine)
	assert.Len(t, stats, 3)
	valid := false
	for _, stream := range stats {
		if stream.Stream == "test-stream" {
			assert.Equal(t, "test-stream", stream.Stream)
			assert.Equal(t, "default", stream.RedisPool)
			assert.Equal(t, uint64(0), stream.Len)
			assert.Len(t, stream.Groups, 0)
			valid = true
		}
	}
	assert.True(t, valid)

	r.XGroupCreateMkStream("test-stream", "test-group", "0")
	flusher := engine.GetEventBroker().NewFlusher()
	type testEvent struct {
		Name string
	}
	for i := 1; i <= 10001; i++ {
		flusher.Publish("test-stream", testEvent{"b"})
	}
	flusher.Flush()
	time.Sleep(time.Millisecond * 500)

	stats = GetRedisStreamsStatistics(engine)
	valid = false
	for _, stream := range stats {
		if stream.Stream == "test-stream" {
			assert.Equal(t, uint64(10001), stream.Len)
			assert.Len(t, stream.Groups, 1)
			assert.Equal(t, "test-group", stream.Groups[0].Group)
			assert.Equal(t, uint64(0), stream.Groups[0].Pending)
			assert.Len(t, stream.Groups[0].Consumers, 0)
			valid = true
		}
	}
	assert.True(t, valid)

	consumer := engine.GetEventBroker().Consumer("test-group")
	consumer.DisableLoop()
	consumer.Consume(11000, func(events []orm.Event) {
		engine.GetRedis().Get("hello")
		engine.GetRedis().Get("hello2")
		engine.GetMysql().Query("SELECT 1")
		time.Sleep(time.Millisecond * 100)
	})

	stats = GetRedisStreamsStatistics(engine)
	valid = false
	for _, stream := range stats {
		if stream.Stream == "test-stream" {
			assert.Equal(t, uint64(10001), stream.Len)
			assert.Len(t, stream.Groups, 1)
			assert.Equal(t, "test-group", stream.Groups[0].Group)
			assert.Equal(t, uint64(0), stream.Groups[0].Pending)
			assert.Len(t, stream.Groups[0].Consumers, 0)
			assert.GreaterOrEqual(t, stream.Groups[0].SpeedMilliseconds, 0.01)
			assert.LessOrEqual(t, stream.Groups[0].SpeedMilliseconds, 0.012)
			assert.GreaterOrEqual(t, stream.Groups[0].RedisQueriesPerEvent, 0.00019)
			assert.LessOrEqual(t, stream.Groups[0].RedisQueriesPerEvent, 0.00021)
			assert.GreaterOrEqual(t, stream.Groups[0].RedisQueriesMillisecondsPerEvent, 0.00001)
			assert.LessOrEqual(t, stream.Groups[0].RedisQueriesMillisecondsPerEvent, 0.001)
			assert.GreaterOrEqual(t, stream.Groups[0].DBQueriesPerEvent, 0.00009)
			assert.LessOrEqual(t, stream.Groups[0].DBQueriesPerEvent, 0.00011)
			assert.GreaterOrEqual(t, stream.Groups[0].DBQueriesMillisecondsPerEvent, 0.00001)
			assert.LessOrEqual(t, stream.Groups[0].DBQueriesMillisecondsPerEvent, 0.0005)
			assert.Len(t, stream.Groups[0].SpeedHistory, 7)
			assert.Equal(t, int64(0), stream.Groups[0].SpeedHistory[0].SpeedEvents)
			valid = true
		}
	}
	assert.True(t, valid)

	flusher.Publish("test-stream", testEvent{"a"})
	flusher.Flush()
	assert.Panics(t, func() {
		consumer.Consume(10, func(events []orm.Event) {
			panic("stop")
		})
	})
	time.Sleep(time.Millisecond * 10)
	stats = GetRedisStreamsStatistics(engine)
	valid = false
	for _, stream := range stats {
		if stream.Stream == "test-stream" {
			assert.Equal(t, uint64(1), stream.Groups[0].Pending)
			valid = true
		}
	}
	assert.True(t, valid)
}
