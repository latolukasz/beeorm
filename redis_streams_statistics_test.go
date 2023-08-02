package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisStreamsStatus(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", "", 11)
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test", MySQLPoolOptions{})
	registry.RegisterRedisStream("test-stream", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream", "test-group")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	c := validatedRegistry.NewContext(context.Background())
	r := c.Engine().Redis()
	r.FlushDB(c)

	stats := c.EventBroker().GetStreamsStatistics()
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

	r.XGroupCreateMkStream(c, "test-stream", "test-group", "0")
	flusher := c.Flusher()
	type testEvent struct {
		Name string
	}
	for i := 1; i <= 10001; i++ {
		flusher.PublishToStream("test-stream", testEvent{"b"}, nil)
	}
	flusher.Flush()
	time.Sleep(time.Millisecond * 500)

	stats = c.EventBroker().GetStreamsStatistics("test-stream")
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

	consumer := c.EventBroker().Consumer("test-group")
	consumer.SetBlockTime(0)
	consumer.Consume(11000, func(events []Event) {
		c.Engine().Redis().Get(c, "hello")
		c.Engine().Redis().Get(c, "hello2")
		c.Engine().DB().Query(c, "SELECT 1")
		time.Sleep(time.Millisecond * 100)
	})

	statsSingle := c.EventBroker().GetStreamStatistics("test-stream")
	assert.Equal(t, uint64(10001), statsSingle.Len)
	assert.Len(t, statsSingle.Groups, 1)
	assert.Equal(t, "test-group", statsSingle.Groups[0].Group)
	assert.Equal(t, uint64(0), statsSingle.Groups[0].Pending)
	assert.Len(t, statsSingle.Groups[0].Consumers, 0)

	flusher.PublishToStream("test-stream", testEvent{"a"}, nil)
	flusher.Flush()
	assert.Panics(t, func() {
		consumer.Consume(10, func(events []Event) {
			panic("stop")
		})
	})
	time.Sleep(time.Millisecond * 10)
	stats = c.EventBroker().GetStreamsStatistics()
	valid = false
	for _, stream := range stats {
		if stream.Stream == "test-stream" {
			assert.Equal(t, uint64(1), stream.Groups[0].Pending)
			valid = true
		}
	}
	assert.True(t, valid)
}
