package beeorm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shamaton/msgpack"
)

type Event interface {
	Ack(c Context)
	ID() string
	Stream() string
	Meta() Meta
	Unserialize(val interface{})
	delete(c Context)
}

type event struct {
	consumer *eventsConsumer
	stream   string
	message  redis.XMessage
	ack      bool
	deleted  bool
	meta     Meta
}

type garbageCollectorEvent struct {
	Group string
	Pool  string
}

func (ev *event) Ack(c Context) {
	ev.consumer.redis.XAck(c, ev.stream, ev.consumer.group, ev.message.ID)
	ev.ack = true
}

func (ev *event) delete(c Context) {
	ev.Ack(c)
	ev.consumer.redis.XDel(c, ev.stream, ev.message.ID)
	ev.deleted = true
}

func (ev *event) ID() string {
	return ev.message.ID
}

func (ev *event) Stream() string {
	return ev.stream
}

func (ev *event) Meta() Meta {
	if ev.meta == nil {
		ev.meta = Meta{}
		for key, value := range ev.message.Values {
			if key != "s" {
				ev.meta[key] = value.(string)
			}
		}
	}
	return ev.meta
}

func (ev *event) Unserialize(value interface{}) {
	val := ev.message.Values["s"]
	err := msgpack.Unmarshal([]byte(val.(string)), &value)
	checkError(err)
}

type EventBroker interface {
	Publish(stream string, body interface{}, meta Meta) (id string)
	Consumer(group string) EventsConsumer
	GetStreamsStatistics(stream ...string) []*RedisStreamStatistics
	GetStreamStatistics(stream string) *RedisStreamStatistics
	GetStreamGroupStatistics(stream, group string) *RedisStreamGroupStatistics
}

type eventBroker struct {
	c *contextImplementation
}

func createEventSlice(body interface{}, meta Meta) []string {
	l := len(meta) * 2
	if body != nil {
		l += 2
	}
	values := make([]string, l)
	i := 0
	for key, value := range meta {
		values[i] = key
		i++
		values[i] = value
		i++
	}

	if body == nil {
		return values
	}
	asString, err := msgpack.Marshal(body)
	checkError(err)
	values[i] = "s"
	values[i+1] = string(asString)
	return values
}

func (c *contextImplementation) EventBroker() EventBroker {
	if c.eventBroker == nil {
		c.eventBroker = &eventBroker{c: c}
	}
	return c.eventBroker
}

func (eb *eventBroker) Publish(stream string, body interface{}, meta Meta) (id string) {
	return eb.c.engine.GetRedis(getRedisCodeForStream(eb.c.engine, stream)).xAdd(eb.c, stream, createEventSlice(body, meta))
}

func getRedisCodeForStream(engine *engineImplementation, stream string) string {
	pool, has := engine.redisStreamPools[stream]
	if !has {
		panic(fmt.Errorf("unregistered stream %s", stream))
	}
	return pool
}

type EventConsumerHandler func(events []Event)

type EventsConsumer interface {
	Consume(c Context, count int, handler EventConsumerHandler) bool
	ConsumeMany(c Context, nr, count int, handler EventConsumerHandler) bool
	Claim(c Context, from, to int)
	SetBlockTime(seconds int)
}

func (eb *eventBroker) Consumer(group string) EventsConsumer {
	streams := eb.c.engine.getRedisStreamsForGroup(group)
	if len(streams) == 0 {
		panic(fmt.Errorf("unregistered streams for group %s", group))
	}
	redisPool := eb.c.engine.registry.redisStreamPools[streams[0]]
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{c: eb.c, block: true, blockTime: time.Second * 30},
		redis:             eb.c.engine.GetRedis(redisPool).(*redisCache),
		streams:           streams,
		group:             group,
		lockTTL:           time.Second * 90,
		lockTick:          time.Minute}
}

type eventConsumerBase struct {
	c         *contextImplementation
	block     bool
	blockTime time.Duration
}

type eventsConsumer struct {
	eventConsumerBase
	redis           *redisCache
	streams         []string
	group           string
	lockTTL         time.Duration
	lockTick        time.Duration
	garbageLastTick int64
}

func (b *eventConsumerBase) SetBlockTime(seconds int) {
	if seconds <= 0 {
		b.block = false
		b.blockTime = -1
		return
	}
	b.block = true
	b.blockTime = time.Duration(seconds) * time.Second
}

func (r *eventsConsumer) Consume(c Context, count int, handler EventConsumerHandler) bool {
	return r.ConsumeMany(c, 1, count, handler)
}

func (r *eventsConsumer) ConsumeMany(c Context, nr, count int, handler EventConsumerHandler) bool {
	return r.consume(c, r.getName(nr), count, handler)
}

func (r *eventsConsumer) consume(c Context, name string, count int, handler EventConsumerHandler) (finished bool) {
	lockKey := r.redis.config.GetNamespace() + r.group + "_" + name
	locker := r.redis.GetLocker()
	lock, has := locker.Obtain(c, lockKey, r.lockTTL, 0)
	if !has {
		return false
	}
	timer := time.NewTimer(r.lockTick)
	defer func() {
		lock.Release(c)
		timer.Stop()
	}()
	r.garbage()

	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(c, stream, r.group, "0")
	}

	attributes := &consumeAttributes{
		Pending:   true,
		BlockTime: r.blockTime,
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
		case <-c.Ctx().Done():
			return true
		case <-timer.C:
			if !lock.Refresh(c, r.lockTTL) {
				return false
			}
			timer.Reset(r.lockTick)
		default:
			if r.digest(c, attributes) {
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

func (r *eventsConsumer) digest(c Context, attributes *consumeAttributes) (stop bool) {
	finished := r.digestKeys(c, attributes)
	if !r.block && finished {
		return true
	}
	return false
}

func (r *eventsConsumer) digestKeys(c Context, attributes *consumeAttributes) (finished bool) {
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
	results := r.redis.XReadGroup(c, a)
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
			if r.block {
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
		r.redis.XAck(c, stream, r.group, ids...)
	}
	return false
}

func (r *eventsConsumer) Claim(c Context, from, to int) {
	for _, stream := range r.streams {
		start := "-"
		for {
			xPendingArg := &redis.XPendingExtArgs{Stream: stream, Group: r.group, Start: start, End: "+", Consumer: r.getName(from), Count: 100}
			pending := r.redis.XPendingExt(c, xPendingArg)
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
			r.redis.XClaimJustID(c, arg)
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
		r.c.EventBroker().Publish(StreamGarbageCollectorChannelName, garbageEvent, nil)
		r.garbageLastTick = now
	}
}
