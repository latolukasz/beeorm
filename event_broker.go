package beeorm

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shamaton/msgpack"

	"github.com/go-redis/redis/v8"
)

const speedHSetKey = "_orm_ss"
const shutdownPollIntervalMax = 500 * time.Millisecond

type Event interface {
	Ack()
	ID() string
	Stream() string
	Tag(key string) (value string)
	Unserialize(val interface{})
	delete()
}

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

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
	if err != nil {
		panic(err)
	}
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
		if grouped[r][stream] == nil {
			grouped[r][stream] = events
		} else {
			grouped[r][stream] = append(grouped[r][stream], events...)
		}
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
type ConsumerErrorHandler func(err error, event Event)

type EventsConsumer interface {
	Consume(count int, handler EventConsumerHandler) bool
	ConsumeMany(nr, count int, handler EventConsumerHandler) bool
	Claim(from, to int)
	DisableLoop()
	SetErrorHandler(handler ConsumerErrorHandler)
	Shutdown(timeout time.Duration)
}

type speedHandler struct {
	DBQueries         int
	DBMicroseconds    int64
	RedisQueries      int
	RedisMicroseconds int64
}

func (s *speedHandler) Handle(fields map[string]interface{}) {
	if fields["source"] == sourceMySQL {
		s.DBQueries++
		s.DBMicroseconds += fields["microseconds"].(int64)
	} else {
		s.RedisQueries++
		s.RedisMicroseconds += fields["microseconds"].(int64)
	}
}

func (s *speedHandler) Clear() {
	s.DBQueries = 0
	s.RedisQueries = 0
	s.DBMicroseconds = 0
	s.RedisMicroseconds = 0
}

func (eb *eventBroker) Consumer(group string) EventsConsumer {
	streams := eb.engine.registry.getRedisStreamsForGroup(group)
	if len(streams) == 0 {
		panic(fmt.Errorf("unregistered streams for group %s", group))
	}
	redisPool := ""
	for _, stream := range streams {
		pool := eb.engine.registry.redisStreamPools[stream]
		if redisPool == "" {
			redisPool = pool
		} else if redisPool != pool {
			panic(fmt.Errorf("reading from different redis pool not allowed"))
		}
	}
	speedPrefixKey := group + "_" + redisPool
	speedLogger := &speedHandler{}
	eb.engine.RegisterQueryLogger(speedLogger, true, true, false)
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{engine: eb.engine, loop: true, blockTime: time.Second * 30},
		redis:             eb.engine.GetRedis(redisPool),
		streams:           streams,
		group:             group,
		lockTTL:           time.Second * 90,
		lockTick:          time.Minute,
		speedLimit:        10000,
		speedPrefixKey:    speedPrefixKey,
		speedLogger:       speedLogger}
}

type eventConsumerBase struct {
	engine       *Engine
	loop         bool
	errorHandler ConsumerErrorHandler
	blockTime    time.Duration
	isRunning    atomicBool
}

type eventsConsumer struct {
	eventConsumerBase
	redis                  *RedisCache
	streams                []string
	group                  string
	lockTTL                time.Duration
	lockTick               time.Duration
	speedPrefixKey         string
	speedEvents            int
	speedDBQueries         int
	speedDBMicroseconds    int64
	speedRedisQueries      int
	speedRedisMicroseconds int64
	speedLogger            *speedHandler
	speedTimeMicroseconds  int64
	speedLimit             int
}

func (b *eventConsumerBase) DisableLoop() {
	b.loop = false
}

func (b *eventConsumerBase) SetErrorHandler(handler ConsumerErrorHandler) {
	b.errorHandler = handler
}

func (r *eventsConsumer) Consume(count int, handler EventConsumerHandler) bool {
	return r.ConsumeMany(1, count, handler)
}

func (r *eventsConsumer) ConsumeMany(nr, count int, handler EventConsumerHandler) bool {
	name := r.getName(nr)
	for {
		golLock, canceled := r.consume(name, count, handler)
		if !golLock {
			return false
		}
		if canceled || !r.loop {
			return true
		}
		time.Sleep(time.Second * 10)
	}
}

func (r *eventsConsumer) consume(name string, count int, handler EventConsumerHandler) (gotLock, canceled bool) {
	lockKey := r.group + "_" + name
	locker := r.redis.GetLocker()
	lock, has := locker.Obtain(lockKey, r.lockTTL, 0)
	if !has {
		return false, false
	}
	r.isRunning.setTrue()
	ticker := time.NewTicker(r.lockTick)
	done := make(chan bool)

	defer func() {
		lock.Release()
		ticker.Stop()
		r.isRunning.setFalse()
		close(done)
	}()
	hasLock := true
	lockAcquired := time.Now()
	go func() {
		for {
			select {
			case <-r.engine.context.Done():
				canceled = true
				return
			case <-done:
				return
			case <-ticker.C:
				if !lock.Refresh(r.lockTTL) {
					hasLock = false
					return
				}
				now := time.Now()
				lockAcquired = now
			}
		}
	}()
	lastIDs := make(map[string]string)
	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(stream, r.group, "0")
	}
	keys := []string{"0", ">"}
	streams := make([]string, len(r.streams)*2)
	b := r.blockTime
	if !r.loop {
		b = -1
	}
	for {
	KEYS:
		for _, key := range keys {
			invalidCheck := key == "0"
			if invalidCheck {
				for _, stream := range r.streams {
					lastIDs[stream] = "0"
				}
			}
			for {
				if canceled {
					return true, canceled
				}
				if !hasLock || time.Since(lockAcquired) > r.lockTTL {
					return false, false
				}

				i := 0
				for _, stream := range r.streams {
					streams[i] = stream
					i++
				}
				for _, stream := range r.streams {
					if invalidCheck {
						streams[i] = lastIDs[stream]
					} else {
						streams[i] = ">"
					}
					i++
				}
				a := &redis.XReadGroupArgs{Consumer: name, Group: r.group, Streams: streams, Count: int64(count), Block: b}
				results := r.redis.XReadGroup(a)
				if canceled {
					return true, canceled
				}
				totalMessages := 0
				for _, row := range results {
					l := len(row.Messages)
					if l > 0 {
						totalMessages += l
						if invalidCheck {
							lastIDs[row.Stream] = row.Messages[l-1].ID
						}
					}
				}
				if totalMessages == 0 {
					continue KEYS
				}
				events := make([]Event, totalMessages)
				i = 0
				for _, row := range results {
					for _, message := range row.Messages {
						events[i] = &event{stream: row.Stream, message: message, consumer: r}
						i++
					}
				}
				r.speedEvents += totalMessages
				r.speedLogger.Clear()
				start := getNow(r.engine.hasRedisLogger)
				func() {
					defer func() {
						if rec := recover(); rec != nil {
							if r.errorHandler != nil {
								finalEvents := make([]Event, 0)
								for _, row := range events {
									e := row.(*event)
									if !e.ack {
										finalEvents = append(finalEvents, row)
									}
								}
								for _, e := range finalEvents {
									func() {
										defer func() {
											if rec := recover(); rec != nil {
												asErr, isError := rec.(error)
												if !isError {
													asErr = fmt.Errorf("%v", rec)
												}
												r.errorHandler(asErr, e)
											}
										}()
										handler([]Event{e})
									}()
								}
								events = make([]Event, 0)
								return
							}
							panic(rec)
						}
					}()
					handler(events)
				}()
				r.speedTimeMicroseconds += time.Since(*start).Microseconds()
				r.speedDBQueries += r.speedLogger.DBQueries
				r.speedRedisQueries += r.speedLogger.RedisQueries
				r.speedDBMicroseconds += r.speedLogger.DBMicroseconds
				r.speedRedisMicroseconds += r.speedLogger.RedisMicroseconds
				var toAck map[string][]string
				allDeleted := true
				for _, ev := range events {
					ev := ev.(*event)
					if !ev.ack {
						if toAck == nil {
							toAck = make(map[string][]string)
						}
						toAck[ev.stream] = append(toAck[ev.stream], ev.message.ID)
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
				if r.speedEvents >= r.speedLimit {
					today := time.Now().Format("01-02-06")
					key := speedHSetKey + today
					pipeline := r.redis.PipeLine()
					pipeline.Expire(key, time.Hour*216)
					pipeline.HIncrBy(key, r.speedPrefixKey+"e", int64(r.speedEvents))
					pipeline.HIncrBy(key, r.speedPrefixKey+"t", r.speedTimeMicroseconds)
					pipeline.HIncrBy(key, r.speedPrefixKey+"d", int64(r.speedDBQueries))
					pipeline.HIncrBy(key, r.speedPrefixKey+"dt", r.speedDBMicroseconds)
					pipeline.HIncrBy(key, r.speedPrefixKey+"r", int64(r.speedRedisQueries))
					pipeline.HIncrBy(key, r.speedPrefixKey+"rt", r.speedRedisMicroseconds)
					pipeline.Exec()
					r.speedEvents = 0
					r.speedDBQueries = 0
					r.speedRedisQueries = 0
					r.speedTimeMicroseconds = 0
					r.speedDBMicroseconds = 0
					r.speedRedisMicroseconds = 0
				}
			}
		}
		if !r.loop {
			break
		}
	}
	return true, false
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

func (b *eventConsumerBase) Shutdown(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pollIntervalBase := time.Millisecond
	nextPollInterval := func() time.Duration {
		interval := pollIntervalBase + time.Duration(rand.Intn(int(pollIntervalBase/10)))
		pollIntervalBase *= 2
		if pollIntervalBase > shutdownPollIntervalMax {
			pollIntervalBase = shutdownPollIntervalMax
		}
		return interval
	}
	timer := time.NewTimer(nextPollInterval())
	defer timer.Stop()
	for {
		if !b.isRunning.isSet() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(nextPollInterval())
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
	garbageEvent := garbageCollectorEvent{Group: r.group, Pool: r.redis.config.GetCode()}
	r.engine.GetEventBroker().Publish(redisStreamGarbageCollectorChannelName, garbageEvent)
}
