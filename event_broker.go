package beeorm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shamaton/msgpack"

	"github.com/go-redis/redis/v8"
)

type Event interface {
	Ack()
	ID() string
	Stream() string
	Tag(key string) (value string)
	Unserialize(val interface{})
	delete()
}

type event struct {
	consumer *eventsConsumer
	stream   string
	message  redis.XMessage
	ack      bool
	deleted  bool
}

type garbageCollectorEvent struct {
	Group string
	Pool  string
}

func (ev *event) Ack() {
	ev.consumer.redis.XAck(ev.stream, ev.consumer.group, ev.message.ID)
	ev.ack = true
}

func (ev *event) delete() {
	ev.Ack()
	ev.consumer.redis.XDel(ev.stream, ev.message.ID)
	ev.deleted = true
}

func (ev *event) ID() string {
	return ev.message.ID
}

func (ev *event) Stream() string {
	return ev.stream
}

func (ev *event) Tag(key string) (value string) {
	val, has := ev.message.Values[key]
	if has {
		return val.(string)
	}
	return ""
}

func (ev *event) Unserialize(value interface{}) {
	val := ev.message.Values["s"]
	err := msgpack.Unmarshal([]byte(val.(string)), &value)
	checkError(err)
}

type EventBroker interface {
	Publish(stream string, body interface{}, meta ...string) (id string)
	Consumer(group string) EventsConsumer
	NewFlusher() EventFlusher
}

type EventFlusher interface {
	Publish(stream string, body interface{}, meta ...string)
	Flush()
}

type eventFlusher struct {
	eb     *eventBroker
	events map[string][][]string
}

type eventBroker struct {
	engine *Engine
}

func createEventSlice(body interface{}, meta []string) []string {
	if body == nil {
		return meta
	}
	asString, err := msgpack.Marshal(body)
	checkError(err)
	values := make([]string, len(meta)+2)
	values[0] = "s"
	values[1] = string(asString)
	for k, v := range meta {
		values[k+2] = v
	}
	return values
}

func (ef *eventFlusher) Publish(stream string, body interface{}, meta ...string) {
	ef.events[stream] = append(ef.events[stream], createEventSlice(body, meta))
}

func (ef *eventFlusher) Flush() {
	grouped := make(map[*RedisCache]map[string][][]string)
	for stream, events := range ef.events {
		r := getRedisForStream(ef.eb.engine, stream)
		if grouped[r] == nil {
			grouped[r] = make(map[string][][]string)
		}
		grouped[r][stream] = events
	}
	for r, events := range grouped {
		p := r.PipeLine()
		for stream, list := range events {
			for _, e := range list {
				p.XAdd(stream, e)
			}
		}
		p.Exec()
	}
	ef.events = make(map[string][][]string)
}

func (e *Engine) GetEventBroker() EventBroker {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	if e.eventBroker == nil {
		e.eventBroker = &eventBroker{engine: e}
	}
	return e.eventBroker
}

func (eb *eventBroker) NewFlusher() EventFlusher {
	return &eventFlusher{eb: eb, events: make(map[string][][]string)}
}

func (eb *eventBroker) Publish(stream string, body interface{}, meta ...string) (id string) {
	return getRedisForStream(eb.engine, stream).xAdd(stream, createEventSlice(body, meta))
}

func getRedisForStream(engine *Engine, stream string) *RedisCache {
	pool, has := engine.registry.redisStreamPools[stream]
	if !has {
		panic(fmt.Errorf("unregistered stream %s", stream))
	}
	return engine.GetRedis(pool)
}

type EventConsumerHandler func([]Event)

type EventsConsumer interface {
	Consume(ctx context.Context, count int, handler EventConsumerHandler) bool
	ConsumeMany(ctx context.Context, nr, count int, handler EventConsumerHandler) bool
	Claim(from, to int)
	DisableLoop()
}

func (eb *eventBroker) Consumer(group string) EventsConsumer {
	streams := eb.engine.registry.getRedisStreamsForGroup(group)
	if len(streams) == 0 {
		panic(fmt.Errorf("unregistered streams for group %s", group))
	}
	redisPool := eb.engine.registry.redisStreamPools[streams[0]]
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{engine: eb.engine, loop: true, blockTime: time.Second * 30},
		redis:             eb.engine.GetRedis(redisPool),
		streams:           streams,
		group:             group,
		lockTTL:           time.Second * 90,
		lockTick:          time.Minute}
}

type eventConsumerBase struct {
	engine    *Engine
	loop      bool
	blockTime time.Duration
}

type eventsConsumer struct {
	eventConsumerBase
	redis           *RedisCache
	streams         []string
	group           string
	lockTTL         time.Duration
	lockTick        time.Duration
	garbageLastTick int64
}

func (b *eventConsumerBase) DisableLoop() {
	b.loop = false
}

func (r *eventsConsumer) Consume(ctx context.Context, count int, handler EventConsumerHandler) bool {
	return r.ConsumeMany(ctx, 1, count, handler)
}

func (r *eventsConsumer) ConsumeMany(ctx context.Context, nr, count int, handler EventConsumerHandler) bool {
	return r.consume(ctx, r.getName(nr), count, handler)
}

func (r *eventsConsumer) consume(ctx context.Context, name string, count int, handler EventConsumerHandler) (finished bool) {
	lockKey := r.group + "_" + name
	locker := r.redis.GetLocker()
	lock, has := locker.Obtain(lockKey, r.lockTTL, 0)
	if !has {
		return false
	}
	timer := time.NewTimer(r.lockTick)
	defer func() {
		lock.Release()
		timer.Stop()
	}()
	r.garbage()

	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(stream, r.group, "0")
	}

	attributes := &consumeAttributes{
		Pending:   true,
		BlockTime: -1,
		Name:      name,
		Count:     count,
		Handler:   handler,
		LastIDs:   make(map[string]string),
		Streams:   make([]string, len(r.streams)*2),
	}
	for _, stream := range r.streams {
		attributes.LastIDs[stream] = "0"
	}
	for {
		select {
		case <-ctx.Done():
			return true
		case <-timer.C:
			if !lock.Refresh(r.lockTTL) {
				return false
			}
			timer.Reset(r.lockTick)
		default:
			if r.digest(ctx, attributes) {
				return true
			}
		}
	}
}

type consumeAttributes struct {
	Pending   bool
	BlockTime time.Duration
	Stop      chan bool
	Name      string
	Count     int
	Handler   EventConsumerHandler
	LastIDs   map[string]string
	Streams   []string
}

func (r *eventsConsumer) digest(ctx context.Context, attributes *consumeAttributes) (stop bool) {
	finished := r.digestKeys(ctx, attributes)
	if !r.loop && finished {
		return true
	}
	return false
}

func (r *eventsConsumer) digestKeys(ctx context.Context, attributes *consumeAttributes) (finished bool) {
	i := 0
	for _, stream := range r.streams {
		attributes.Streams[i] = stream
		i++
	}
	for _, stream := range r.streams {
		if attributes.Pending {
			attributes.Streams[i] = attributes.LastIDs[stream]
		} else {
			attributes.Streams[i] = ">"
		}
		i++
	}
	a := &redis.XReadGroupArgs{Consumer: attributes.Name, Group: r.group, Streams: attributes.Streams,
		Count: int64(attributes.Count), Block: attributes.BlockTime}
	results := r.redis.XReadGroup(ctx, a)
	totalMessages := 0
	for _, row := range results {
		l := len(row.Messages)
		if l > 0 {
			totalMessages += l
			if attributes.Pending {
				attributes.LastIDs[row.Stream] = row.Messages[l-1].ID
			}
		}
	}
	if totalMessages == 0 {
		if attributes.Pending {
			attributes.Pending = false
			if r.loop {
				attributes.BlockTime = r.blockTime
			}
			return false
		}
		return true
	}
	events := make([]Event, totalMessages)
	i = 0
	for _, row := range results {
		for _, message := range row.Messages {
			events[i] = &event{stream: row.Stream, message: message, consumer: r}
			i++
		}
	}
	attributes.Handler(events)
	var toAck map[string][]string
	allDeleted := true
	for _, ev := range events {
		ev := ev.(*event)
		if !ev.ack {
			if toAck == nil {
				toAck = make(map[string][]string)
			}
			toAck[ev.stream] = append(toAck[ev.stream], ev.message.ID)
			allDeleted = false
		} else if !ev.deleted {
			allDeleted = false
		}
	}
	if !allDeleted {
		r.garbage()
	}
	for stream, ids := range toAck {
		r.redis.XAck(stream, r.group, ids...)
	}
	return false
}

func (r *eventsConsumer) Claim(from, to int) {
	for _, stream := range r.streams {
		start := "-"
		for {
			xPendingArg := &redis.XPendingExtArgs{Stream: stream, Group: r.group, Start: start, End: "+", Consumer: r.getName(from), Count: 100}
			pending := r.redis.XPendingExt(xPendingArg)
			l := len(pending)
			if l == 0 {
				break
			}
			ids := make([]string, l)
			for i, row := range pending {
				ids[i] = row.ID
			}
			start = r.incrementID(ids[l-1])
			arg := &redis.XClaimArgs{Consumer: r.getName(to), Stream: stream, Group: r.group, Messages: ids}
			r.redis.XClaimJustID(arg)
			if l < 100 {
				break
			}
		}
	}
}

func (r *eventsConsumer) getName(nr int) string {
	return "consumer-" + strconv.Itoa(nr)
}

func (r *eventsConsumer) incrementID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return s[0] + "-" + strconv.Itoa(counter+1)
}

func (r *eventsConsumer) garbage() {
	now := time.Now().Unix()
	if (now - r.garbageLastTick) >= 10 {
		garbageEvent := garbageCollectorEvent{Group: r.group, Pool: r.redis.config.GetCode()}
		r.engine.GetEventBroker().Publish(RedisStreamGarbageCollectorChannelName, garbageEvent)
		r.garbageLastTick = now
	}
}
