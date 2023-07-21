package beeorm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

func TestRedisStreamGroupConsumerClean(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", "", 15)
	registry.RegisterRedisStream("test-stream", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream", "test-group-1", "test-group-2")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	c := validatedRegistry.NewContext(context.Background())
	c.Engine().GetRedis("").FlushDB(c)
	broker := c.EventBroker()
	flusher := c.Flusher()
	type testEvent struct {
		Name string
	}
	for i := 1; i <= 10; i++ {
		flusher.PublishToStream("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	flusher.Flush()

	consumer1 := broker.Consumer("test-group-1")
	consumer1.SetBlockTime(0)
	consumer2 := broker.Consumer("test-group-2")
	consumer2.SetBlockTime(0)

	consumer1.Consume(1, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)
	assert.Equal(t, int64(10), c.Engine().GetRedis("").XLen(c, "test-stream"))

	consumer2.Consume(1, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)

	RunLazyFlushConsumer(c, true)
	assert.Equal(t, int64(0), c.Engine().GetRedis("").XLen(c, "test-stream"))

	for i := 1; i <= 10; i++ {
		flusher.PublishToStream("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	flusher.Flush()
	assert.PanicsWithError(t, "stop", func() {
		consumer2.Consume(10, func(events []Event) {
			events[0].Ack(c)
			panic(fmt.Errorf("stop"))
		})
	})
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", "", 15)
	registry.RegisterRedisStream("test-stream", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream", "test-group")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	c := validatedRegistry.NewContext(context.Background())
	c.Engine().GetRedis("").FlushDB(c)
	broker := c.EventBroker()

	consumer := broker.Consumer("test-group")
	consumer.SetBlockTime(0)
	consumer.Consume(1, func(events []Event) {})
	consumer.Consume(1, func(events []Event) {})
	type testEvent struct {
		Name string
	}

	c.Engine().GetRedis("").FlushDB(c)
	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	iterations1 := false
	iterations2 := false
	consumed1 := false
	consumed2 := false
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-group")
		consumer.SetBlockTime(0)
		consumed1 = consumer.Consume(5, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-group")
		consumer.SetBlockTime(0)
		consumed2 = consumer.Consume(5, func(events []Event) {
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.False(t, iterations2)
	assert.True(t, consumed1)
	assert.False(t, consumed2)

	c.Engine().GetRedis("").FlushDB(c)
	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	iterations1 = false
	iterations2 = false
	consumed1 = false
	consumed2 = false
	wg = &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-group")
		consumer.SetBlockTime(0)
		consumed1 = consumer.ConsumeMany(1, 5, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
			assert.NotEmpty(t, events[0].ID())
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-group")
		consumer.SetBlockTime(0)
		consumed2 = consumer.ConsumeMany(2, 5, func(events []Event) {
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)
	assert.True(t, consumed1)
	assert.True(t, consumed2)

	c.Engine().GetRedis("").FlushDB(c)
	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	consumer = broker.Consumer("test-group")
	consumer.SetBlockTime(0)
	assert.PanicsWithError(t, "stop", func() {
		consumed2 = consumer.ConsumeMany(1, 3, func(events []Event) {
			panic(errors.New("stop"))
		})
	})
	pending := c.Engine().GetRedis("").XPending(c, "test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers["consumer-1"])

	consumer.Claim(1, 2)
	pending = c.Engine().GetRedis("").XPending(c, "test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers["consumer-2"])
	consumer.Claim(7, 2)

	consumer = broker.Consumer("test-group")
	consumer.SetBlockTime(0)
}

func TestRedisStreamGroupConsumer(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", "", 11)
	registry.RegisterRedisStream("test-stream", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream", "test-group")
	registry.RegisterRedisStream("test-stream-a", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream-a", "test-group", "test-group-multi")

	registry.RegisterRedisStream("test-stream-b", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream-b", "test-group", "test-group-multi")

	ctx, cancel := context.WithCancel(context.Background())
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	c := validatedRegistry.NewContext(context.Background())
	c.Engine().GetRedis("").FlushDB(c)
	broker := c.EventBroker()

	consumer := broker.Consumer("test-group")
	consumer.SetBlockTime(0)
	consumer.Consume(5, func(events []Event) {})

	type testEvent struct {
		Name string
	}
	e := &testEvent{}

	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	iterations := 0
	consumer.Consume(5, func(events []Event) {
		iterations++
		assert.Len(t, events, 5)
		if iterations == 1 {
			events[0].Unserialize(e)
			assert.Equal(t, "a1", e.Name)
			events[1].Unserialize(e)
			assert.Equal(t, "a2", e.Name)
			events[2].Unserialize(e)
			assert.Equal(t, "a3", e.Name)
			events[3].Unserialize(e)
			assert.Equal(t, "a4", e.Name)
			events[4].Unserialize(e)
			assert.Equal(t, "a5", e.Name)
		} else {
			events[0].Unserialize(e)
			assert.Equal(t, "a6", e.Name)
			events[1].Unserialize(e)
			assert.Equal(t, "a7", e.Name)
			events[2].Unserialize(e)
			assert.Equal(t, "a8", e.Name)
			events[3].Unserialize(e)
			assert.Equal(t, "a9", e.Name)
			events[4].Unserialize(e)
			assert.Equal(t, "a10", e.Name)
		}
	})
	assert.Equal(t, 2, iterations)
	time.Sleep(time.Millisecond * 20)
	RunLazyFlushConsumer(c, true)
	time.Sleep(time.Second)
	assert.Equal(t, int64(0), c.Engine().GetRedis("").XLen(c, "test-stream"))
	assert.Equal(t, int64(0), c.Engine().GetRedis("").XInfoGroups(c, "test-stream")[0].Pending)
	iterations = 0
	consumer.Consume(10, func(events []Event) {
		iterations++
		assert.Len(t, events, 10)
	})
	assert.Equal(t, 0, iterations)

	c.Engine().GetRedis("").XTrim(c, "test-stream", 0)
	for i := 11; i <= 20; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	iterations = 0
	consumer.Consume(5, func(events []Event) {
		iterations++
		assert.Len(t, events, 5)
		if iterations == 1 {
			events[0].Unserialize(e)
			assert.Equal(t, "a11", e.Name)
		} else {
			events[0].Unserialize(e)
			assert.Equal(t, "a16", e.Name)
		}
	})
	assert.Equal(t, 2, iterations)
	assert.Equal(t, int64(10), c.Engine().GetRedis("").XLen(c, "test-stream"))
	assert.Equal(t, int64(0), c.Engine().GetRedis("").XInfoGroups(c, "test-stream")[0].Pending)
	iterations = 0

	c.Engine().GetRedis("").FlushDB(c)
	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	consumer = broker.Consumer("test-group-multi")
	consumer.SetBlockTime(0)
	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream-a", testEvent{fmt.Sprintf("a%d", i)}, nil)
		c.EventBroker().Publish("test-stream-b", testEvent{fmt.Sprintf("b%d", i)}, nil)
	}
	consumer.Consume(8, func(events []Event) {
		iterations++
		if iterations == 1 {
			assert.Len(t, events, 16)
		} else {
			assert.Len(t, events, 4)
		}
	})
	assert.Equal(t, 2, iterations)

	c.Engine().GetRedis("").FlushDB(c)
	iterations = 0
	messages := 0
	valid := false
	consumer = broker.Consumer("test-group")
	for i := 1; i <= 10; i++ {
		c.EventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)}, nil)
	}
	go func() {
		consumer = broker.Consumer("test-group")
		consumer.SetBlockTime(0)
		consumer.Consume(8, func(events []Event) {
			iterations++
			messages += len(events)
		})
	}()
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 10, messages)

	c.Engine().GetRedis("").FlushDB(c)
	c = validatedRegistry.NewContext(ctx)
	broker = c.EventBroker()
	consumer = broker.Consumer("test-group")
	consumer.SetBlockTime(1)
	valid = true
	go func() {
		time.Sleep(time.Millisecond * 200)
		cancel()
	}()
	consumer.Consume(1, func(events []Event) {
		valid = false
	})
	assert.True(t, valid)
	c.Engine().GetRedis("").Del(c, "test-stream")

	type testStructEvent struct {
		Name string
		Age  int
	}

	c = validatedRegistry.NewContext(context.Background())
	flusher := c.Flusher()
	flusher.PublishToStream("test-stream", testStructEvent{Name: "a", Age: 18}, nil)
	flusher.PublishToStream("test-stream", testStructEvent{Name: "b", Age: 20}, nil)
	flusher.Flush()
	valid = false
	consumer = c.EventBroker().Consumer("test-group")
	consumer.SetBlockTime(0)
	consumer.Consume(10, func(events []Event) {
		valid = true
		assert.Len(t, events, 2)
		for i, event := range events {
			data := &testStructEvent{}
			event.Unserialize(data)
			if i == 0 {
				assert.Equal(t, "a", data.Name)
				assert.Equal(t, 18, data.Age)
			} else {
				assert.Equal(t, "b", data.Name)
				assert.Equal(t, 20, data.Age)
			}
		}
	})
	assert.True(t, valid)

	flusher.PublishToStream("test-stream", "test", Meta{"tag": "val1", "tag2": "val2"})
	flusher.PublishToStream("test-stream", nil, Meta{"tag3": "val3"})
	flusher.Flush()
	valid = false
	consumer.Consume(10, func(events []Event) {
		valid = true
		assert.Len(t, events, 2)
		data := ""
		events[0].Unserialize(&data)
		assert.Equal(t, "test", data)
		assert.Equal(t, "val1", events[0].Meta()["tag"])
		assert.Equal(t, "val2", events[0].Meta()["tag2"])
		assert.Equal(t, "", events[0].Meta()["tag3"])
		assert.Equal(t, "val3", events[1].Meta()["tag3"])
	})
	assert.True(t, valid)

	ctxCancel, stop := context.WithCancel(context.Background())
	defer stop()
	c = validatedRegistry.NewContext(ctxCancel)
	c.Engine().GetRedis("").FlushDB(c)
	flusher = c.Flusher()
	for i := 0; i < 100; i++ {
		flusher.PublishToStream("test-stream", "a", nil)
	}
	flusher.Flush()
	broker = c.EventBroker()
	consumer = broker.Consumer("test-group")
	incr := 0
	start := time.Now()
	go func() {
		consumer.ConsumeMany(1, 1, func(events []Event) {
			incr++
			time.Sleep(time.Millisecond * 50)
		})
	}()
	time.Sleep(time.Millisecond * 200)
	stop()
	assert.Equal(t, 4, incr)
	assert.Less(t, time.Since(start).Milliseconds(), int64(500))

	ctxCancel, stop2 := context.WithCancel(context.Background())
	defer stop2()
	c = validatedRegistry.NewContext(context.Background())
	c.Engine().GetRedis("").FlushDB(c)
	flusher = c.Flusher()
	flusher.PublishToStream("test-stream", "a", nil)
	flusher.Flush()
	broker = c.EventBroker()

	assert.PanicsWithError(t, "unregistered stream test-stream-invalid", func() {
		flusher.PublishToStream("test-stream-invalid", testStructEvent{Name: "a", Age: 18}, nil)
	})
	assert.PanicsWithError(t, "unregistered streams for group test-group-invalid", func() {
		broker.Consumer("test-group-invalid")
	})
}
