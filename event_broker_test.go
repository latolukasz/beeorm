package beeorm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisStreamGroupConsumerClean(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group-1", "test-group-2"})
	ctx := context.Background()
	validatedRegistry, def, err := registry.Validate(ctx)
	assert.NoError(t, err)
	defer def()
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()
	eventFlusher := engine.GetEventBroker().NewFlusher()
	type testEvent struct {
		Name string
	}
	for i := 1; i <= 10; i++ {
		eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()

	consumer1 := broker.Consumer("test-group-1")
	consumer1.(*eventsConsumer).blockTime = time.Millisecond
	consumer1.DisableLoop()
	consumer2 := broker.Consumer("test-group-2")
	consumer2.(*eventsConsumer).blockTime = time.Millisecond
	consumer2.DisableLoop()

	consumer1.Consume(1, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))

	consumer2.Consume(1, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)
	consumer2.(*eventsConsumer).garbage()

	backgroundConsumer := NewBackgroundConsumer(engine)
	backgroundConsumer.DisableLoop()
	backgroundConsumer.blockTime = time.Millisecond
	backgroundConsumer.Digest()
	assert.Equal(t, int64(0), engine.GetRedis().XLen("test-stream"))

	for i := 1; i <= 10; i++ {
		eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()
	consumer1.Consume(100, func(events []Event) {})
	consumer2.Consume(100, func(events []Event) {})
	time.Sleep(time.Millisecond * 200)
	consumer2.(*eventsConsumer).garbageLastTick = 0
	consumer2.(*eventsConsumer).garbage()
	engine.GetRedis().Del("test-group-2_gc")
	backgroundConsumer = NewBackgroundConsumer(engine)
	backgroundConsumer.DisableLoop()
	backgroundConsumer.blockTime = time.Millisecond
	backgroundConsumer.Digest()
	assert.Equal(t, int64(0), engine.GetRedis().XLen("test-stream"))
	consumer2.(*eventsConsumer).garbage()

	for i := 1; i <= 10; i++ {
		eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()
	assert.PanicsWithError(t, "stop", func() {
		consumer2.Consume(10, func(events []Event) {
			events[0].Ack()
			panic(fmt.Errorf("stop"))
		})
	})
	consumer1.Consume(100, func(events []Event) {})
	consumer2.(*eventsConsumer).garbageLastTick = 0
	consumer2.(*eventsConsumer).garbage()
	engine.GetRedis().Del("test-group-2_gc")
	backgroundConsumer.Digest()
	assert.Equal(t, int64(9), engine.GetRedis().XLen("test-stream"))
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	ctx := context.Background()
	validatedRegistry, def, err := registry.Validate(ctx)
	assert.NoError(t, err)
	defer def()
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()

	consumer := broker.Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
	consumer.Consume(1, func(events []Event) {})
	consumer.Consume(1, func(events []Event) {})
	type testEvent struct {
		Name string
	}

	engine.GetRedis().FlushDB()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
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
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableLoop()
		consumed1 = consumer.Consume(5, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableLoop()
		consumed2 = consumer.Consume(5, func(events []Event) {
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.False(t, iterations2)
	assert.True(t, consumed1)
	assert.False(t, consumed2)

	engine.GetRedis().FlushDB()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
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
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableLoop()
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
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableLoop()
		consumed2 = consumer.ConsumeMany(2, 5, func(events []Event) {
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)
	assert.True(t, consumed1)
	assert.True(t, consumed2)

	engine.GetRedis().FlushDB()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	consumer = broker.Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
	assert.PanicsWithError(t, "stop", func() {
		consumed2 = consumer.ConsumeMany(1, 3, func(events []Event) {
			panic("stop")
		})
	})
	pending := engine.GetRedis().XPending("test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers["consumer-1"])

	consumer.Claim(1, 2)
	pending = engine.GetRedis().XPending("test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers["consumer-2"])
	consumer.Claim(7, 2)

	consumer = broker.Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
}

func TestRedisStreamGroupConsumer(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 11)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	registry.RegisterRedisStream("test-stream-a", "default", []string{"test-group", "test-group-multi"})
	registry.RegisterRedisStream("test-stream-b", "default", []string{"test-group", "test-group-multi"})
	assert.PanicsWithError(t, "stream with name test-stream already exists", func() {
		registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	})
	ctx, cancel := context.WithCancel(context.Background())
	validatedRegistry, def, err := registry.Validate(ctx)
	assert.NoError(t, err)
	defer def()
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()

	consumer := broker.Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
	consumer.DisableLoop()
	consumer.Consume(5, func(events []Event) {})

	type testEvent struct {
		Name string
	}
	e := &testEvent{}

	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	iterations := 0
	speedLimitBefore := consumer.(*eventsConsumer).speedLimit
	consumer.(*eventsConsumer).speedLimit = 10
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
	consumer.(*eventsConsumer).speedLimit = speedLimitBefore
	today := time.Now().Format("01-02-06")
	statsEvents, hasStats := engine.GetRedis().HGet(speedHSetKey+today, "test-group_defaulte")
	assert.True(t, hasStats)
	assert.Equal(t, "10", statsEvents)
	assert.Equal(t, 2, iterations)
	time.Sleep(time.Millisecond * 20)
	consumer.(*eventsConsumer).garbage()
	backgroundConsumer := NewBackgroundConsumer(engine)
	backgroundConsumer.DisableLoop()
	backgroundConsumer.blockTime = time.Millisecond
	backgroundConsumer.Digest()
	time.Sleep(time.Second)
	assert.Equal(t, int64(0), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(0), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)
	iterations = 0
	consumer.Consume(10, func(events []Event) {
		iterations++
		assert.Len(t, events, 10)
	})
	assert.Equal(t, 0, iterations)

	engine.GetRedis().XTrim("test-stream", 0)
	for i := 11; i <= 20; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
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
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(0), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)
	iterations = 0

	engine.GetRedis().FlushDB()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	consumer = broker.Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	iterations = 0
	assert.Equal(t, 0, iterations)
	engine.GetRedis().FlushDB()
	iterations = 0
	consumer = broker.Consumer("test-group-multi")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream-a", testEvent{fmt.Sprintf("a%d", i)})
		engine.GetEventBroker().Publish("test-stream-b", testEvent{fmt.Sprintf("b%d", i)})
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

	engine.GetRedis().FlushDB()
	iterations = 0
	messages := 0
	valid := false
	consumer = broker.Consumer("test-group")
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	go func() {
		consumer = broker.Consumer("test-group")
		consumer.DisableLoop()
		consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
		consumer.Consume(8, func(events []Event) {
			iterations++
			messages += len(events)
		})
	}()
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 10, messages)

	engine.GetRedis().FlushDB()
	consumer = broker.Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 400
	valid = true
	go func() {
		time.Sleep(time.Millisecond * 200)
		cancel()
	}()
	consumer.Consume(1, func(events []Event) {
		valid = false
	})
	assert.True(t, valid)

	type testStructEvent struct {
		Name string
		Age  int
	}

	engine = validatedRegistry.CreateEngine(context.Background())
	eventFlusher := engine.GetEventBroker().NewFlusher()
	eventFlusher.Publish("test-stream", testStructEvent{Name: "a", Age: 18})
	eventFlusher.Publish("test-stream", testStructEvent{Name: "b", Age: 20})
	eventFlusher.Flush()
	valid = false
	consumer = engine.GetEventBroker().Consumer("test-group")
	consumer.DisableLoop()
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
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

	eventFlusher.Publish("test-stream", "test", "tag", "val1", "tag2", "val2")
	eventFlusher.Publish("test-stream", nil, "tag3", "val3")
	eventFlusher.Flush()
	valid = false
	consumer.Consume(10, func(events []Event) {
		valid = true
		assert.Len(t, events, 2)
		data := ""
		events[0].Unserialize(&data)
		assert.Equal(t, "test", data)
		assert.Equal(t, "val1", events[0].Tag("tag"))
		assert.Equal(t, "val2", events[0].Tag("tag2"))
		assert.Equal(t, "", events[0].Tag("tag3"))
		assert.Equal(t, "val3", events[1].Tag("tag3"))
	})
	assert.True(t, valid)

	ctxCancel, stop := context.WithCancel(context.Background())
	defer stop()
	engine = validatedRegistry.CreateEngine(ctxCancel)
	engine.GetRedis().FlushDB()
	eventFlusher = engine.GetEventBroker().NewFlusher()
	for i := 0; i < 100; i++ {
		eventFlusher.Publish("test-stream", "a")
	}
	eventFlusher.Flush()
	broker = engine.GetEventBroker()
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
	engine.EnableQueryDebug()
	stop()
	assert.Equal(t, 4, incr)
	assert.Less(t, time.Since(start).Milliseconds(), int64(500))

	ctxCancel, stop2 := context.WithCancel(context.Background())
	defer stop2()
	engine = validatedRegistry.CreateEngine(ctxCancel)
	engine.GetRedis().FlushDB()
	eventFlusher = engine.GetEventBroker().NewFlusher()
	eventFlusher.Publish("test-stream", "a")
	eventFlusher.Flush()
	broker = engine.GetEventBroker()
	consumer = broker.Consumer("test-group")
	incr = 0
	start = time.Now()
	go func() {
		consumer.ConsumeMany(1, 1, func(events []Event) {
			incr++
			time.Sleep(time.Millisecond * 300)
		})
	}()
	time.Sleep(time.Millisecond * 100)
	stop()
	consumer.Shutdown(time.Millisecond * 20)
	assert.Equal(t, 1, incr)
	assert.Less(t, time.Since(start).Milliseconds(), int64(300))

	eventFlusher.Publish("test-stream-invalid", testStructEvent{Name: "a", Age: 18})
	assert.PanicsWithError(t, "unregistered stream test-stream-invalid", func() {
		eventFlusher.Flush()
	})
	assert.PanicsWithError(t, "unregistered streams for group test-group-invalid", func() {
		broker.Consumer("test-group-invalid")
	})

	ctxWithTimeout, cancel2 := context.WithTimeout(context.Background(), time.Millisecond*300)
	defer cancel2()
	engine = validatedRegistry.CreateEngine(ctxWithTimeout)
	consumer = engine.GetEventBroker().Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
	consumer.(*eventsConsumer).lockTick = time.Millisecond * 10
	start = time.Now()
	res := consumer.Consume(5, func(events []Event) {})
	assert.False(t, res)

	ctxWithTimeout2, cancel2 := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel2()
	engine = validatedRegistry.CreateEngine(ctxWithTimeout2)
	consumer = engine.GetEventBroker().Consumer("test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 200
	consumer.(*eventsConsumer).lockTick = time.Millisecond * 100
	go func() {
		time.Sleep(time.Millisecond * 200)
		engine.GetRedis().Del("test-group_consumer-1")
	}()
	now := time.Now()
	engine.GetRedis().Del("test-group_consumer-1")
	res = consumer.Consume(5, func(events []Event) {})
	assert.False(t, res)
	assert.LessOrEqual(t, time.Since(now).Milliseconds(), int64(1000))
}
