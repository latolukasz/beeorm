package beeorm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shamaton/msgpack"

	"github.com/go-redis/redis/v8"
)

const countPending = 100
const pendingClaimCheckDuration = time.Minute * 2
const speedHSetKey = "_orm_ss"

type Event interface {
	Ack()
	ID() string
	Stream() string
	Tag(key string) (value string)
	Unserialize(val interface{})
}

type event struct {
	consumer *eventsConsumer
	stream   string
	message  redis.XMessage
	ack      bool
}

func (ev *event) Ack() {
	ev.consumer.redis.XAck(ev.stream, ev.consumer.group, ev.message.ID)
	ev.ack = true
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
	Consume(count int, handler EventConsumerHandler)
	DisableLoop()
	SetLimit(limit int)
	SetErrorHandler(handler ConsumerErrorHandler)
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
	streams := make([]string, 0)
	for _, row := range eb.engine.registry.redisStreamGroups {
		for stream, groups := range row {
			_, has := groups[group]
			if has {
				streams = append(streams, stream)
			}
		}
	}
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
	return &eventsConsumer{eventConsumerBase: eventConsumerBase{engine: eb.engine, loop: true, limit: 1, blockTime: time.Second * 30},
		redis: eb.engine.GetRedis(redisPool), streams: streams, group: group,
		lockTTL: time.Second * 90, lockTick: time.Minute,
		garbageTick: time.Second * 30, minIdle: pendingClaimCheckDuration,
		claimDuration: pendingClaimCheckDuration, speedLimit: 10000, speedPrefixKey: speedPrefixKey,
		speedLogger: speedLogger}
}

type eventConsumerBase struct {
	engine       *Engine
	loop         bool
	limit        int
	errorHandler ConsumerErrorHandler
	blockTime    time.Duration
}

type eventsConsumer struct {
	eventConsumerBase
	redis                  *RedisCache
	nr                     int
	speedPrefixKey         string
	deadConsumers          int
	speedEvents            int
	speedDBQueries         int
	speedDBMicroseconds    int64
	speedRedisQueries      int
	speedRedisMicroseconds int64
	speedLogger            *speedHandler
	speedTimeMicroseconds  int64
	speedLimit             int
	nrString               string
	streams                []string
	group                  string
	lockTTL                time.Duration
	lockTick               time.Duration
	garbageTick            time.Duration
	minIdle                time.Duration
	claimDuration          time.Duration
	garbageCollectorSha1   string
}

func (b *eventConsumerBase) DisableLoop() {
	b.loop = false
}

func (b *eventConsumerBase) SetLimit(limit int) {
	b.limit = limit
}

func (b *eventConsumerBase) SetErrorHandler(handler ConsumerErrorHandler) {
	b.errorHandler = handler
}

func (r *eventsConsumer) Consume(count int, handler EventConsumerHandler) {
	for {
		valid := r.consume(count, handler)
		if valid || !r.loop {
			break
		}
		time.Sleep(time.Second * 10)
	}
}

func (r *eventsConsumer) consume(count int, handler EventConsumerHandler) bool {
	uniqueLockKey := r.group + "_" + r.redis.config.GetCode()
	runningKey := uniqueLockKey + "_running"
	locker := r.redis.GetLocker()
	nr := 0
	var lockName string
	var lock *Lock
	for {
		nr++
		lockName = uniqueLockKey + "-" + strconv.Itoa(nr)
		locked, has := locker.Obtain(lockName, r.lockTTL, 0)
		if !has {
			if nr < r.limit {
				continue
			}
			panic(fmt.Errorf("consumer for group %s limit %d reached", r.group, r.limit))
		}
		lock = locked
		r.nr = nr
		r.nrString = strconv.Itoa(nr)
		r.redis.HSet(runningKey, r.nrString, strconv.FormatInt(time.Now().Unix(), 10))
		break
	}
	ticker := time.NewTicker(r.lockTick)
	done := make(chan bool)

	defer func() {
		lock.Release()
		ticker.Stop()
		close(done)
		r.deadConsumers = 0
	}()
	hasLock := true
	canceled := false
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
				r.redis.HSet(runningKey, r.nrString, strconv.FormatInt(now.Unix(), 10))
			}
		}
	}()
	garbageTicker := time.NewTicker(r.garbageTick)
	subEngine := r.redis.engine.Clone()
	go func() {
		r.garbageCollector(subEngine)
		for {
			select {
			case <-r.redis.engine.context.Done():
				return
			case <-done:
				return
			case <-garbageTicker.C:
				r.garbageCollector(subEngine)
			}
		}
	}()

	lastIDs := make(map[string]string)
	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(stream, r.group, "0")
	}
	keys := []string{"pending", "0", ">"}
	streams := make([]string, len(r.streams)*2)
	pendingChecked := false
	var pendingCheckedTime time.Time
	b := r.blockTime
	if !r.loop {
		b = -1
	}
	for {
	KEYS:
		for _, key := range keys {
			invalidCheck := key == "0"
			pendingCheck := key == "pending"
			if pendingCheck {
				if pendingChecked && time.Since(pendingCheckedTime) < r.claimDuration {
					continue
				}

				r.deadConsumers = 0
				all := r.redis.HGetAll(runningKey)
				for k, v := range all {
					if k == r.nrString {
						continue
					}
					unix, _ := strconv.ParseInt(v, 10, 64)
					if time.Since(time.Unix(unix, 0)) >= r.claimDuration {
						r.deadConsumers++
					}
				}
				if r.deadConsumers == 0 {
					continue
				}
				for _, stream := range r.streams {
					start := "-"
					for {
						end := strconv.FormatInt(time.Now().Add(-r.minIdle).UnixNano()/1000000, 10)
						pending := r.redis.XPendingExt(&redis.XPendingExtArgs{Stream: stream, Group: r.group, Start: start, End: end, Count: countPending})
						if len(pending) == 0 {
							break
						}
						ids := make([]string, 0)
						for _, row := range pending {
							if row.Consumer != r.getName() && row.Idle >= r.minIdle {
								ids = append(ids, row.ID)
							}
							start = r.incrementID(row.ID)
						}
						if len(ids) > 0 {
							arg := &redis.XClaimArgs{Consumer: r.getName(), Stream: stream, Group: r.group, MinIdle: r.minIdle, Messages: ids}
							r.redis.XClaimJustID(arg)
						}
						if len(pending) < countPending {
							break
						}
					}
				}
				pendingChecked = true
				pendingCheckedTime = time.Now()
				continue
			}
			if invalidCheck {
				for _, stream := range r.streams {
					lastIDs[stream] = "0"
				}
			}
			for {
				if canceled {
					return true
				}
				if !hasLock || time.Since(lockAcquired) > r.lockTTL {
					return false
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
				a := &redis.XReadGroupArgs{Consumer: r.getName(), Group: r.group, Streams: streams, Count: int64(count), Block: b}
				results := r.redis.XReadGroup(a)
				if canceled {
					return true
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
				totalACK := 0
				var toAck map[string][]string
				for _, ev := range events {
					ev := ev.(*event)
					if ev.ack {
						totalACK++
					} else {
						if toAck == nil {
							toAck = make(map[string][]string)
						}
						toAck[ev.stream] = append(toAck[ev.stream], ev.message.ID)
						totalACK++
					}
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
				if r.deadConsumers > 0 && time.Since(pendingCheckedTime) >= r.claimDuration {
					break
				}
			}
		}
		if !r.loop {
			break
		}
	}
	return true
}

func (r *eventsConsumer) getName() string {
	return r.group + "-" + r.nrString
}

func (r *eventsConsumer) incrementID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return s[0] + "-" + strconv.Itoa(counter+1)
}

func (r *eventsConsumer) garbageCollector(engine *Engine) {
	redisGarbage := engine.GetRedis(r.redis.config.GetCode())
	if r.limit > 1 {
		lockKey := r.group + "_" + r.redis.config.GetCode()
		_, has := redisGarbage.GetLocker().Obtain(lockKey, time.Second*20, 0)
		if !has {
			return
		}
	}
	def := engine.registry.redisStreamGroups[redisGarbage.config.GetCode()]
	for _, stream := range r.streams {
		info := redisGarbage.XInfoGroups(stream)
		ids := make(map[string][]int64)
		for name := range def[stream] {
			ids[name] = []int64{0, 0}
		}
		inPending := false
		for _, group := range info {
			_, has := ids[group.Name]
			if !has {
				continue
			}
			if group.LastDeliveredID == "" {
				continue
			}
			lastDelivered := group.LastDeliveredID
			pending := redisGarbage.XPending(stream, group.Name)
			if pending.Lower != "" {
				lastDelivered = pending.Lower
				inPending = true
			}
			s := strings.Split(lastDelivered, "-")
			id, _ := strconv.ParseInt(s[0], 10, 64)
			ids[group.Name][0] = id
			counter, _ := strconv.ParseInt(s[1], 10, 64)
			ids[group.Name][1] = counter
		}
		minID := []int64{-1, 0}
		for _, id := range ids {
			if id[0] == 0 {
				minID[0] = 0
				minID[1] = 0
			} else if minID[0] == -1 || id[0] < minID[0] || (id[0] == minID[0] && id[1] < minID[1]) {
				minID[0] = id[0]
				minID[1] = id[1]
			}
		}
		if minID[0] == 0 {
			continue
		}
		// TODO check of redis 6.2 and use trim with minid
		var end string
		if inPending {
			if minID[1] > 0 {
				end = strconv.FormatInt(minID[0], 10) + "-" + strconv.FormatInt(minID[1]-1, 10)
			} else {
				end = strconv.FormatInt(minID[0]-1, 10)
			}
		} else {
			end = strconv.FormatInt(minID[0], 10) + "-" + strconv.FormatInt(minID[1], 10)
		}

		if r.garbageCollectorSha1 == "" {
			sha1, has := redisGarbage.Get("_orm_gc_sha1")
			if !has {
				script := `
						local count = 0
						local all = 0
						while(true)
						do
							local T = redis.call('XRANGE', KEYS[1], "-", ARGV[1], "COUNT", 1000)
							local ids = {}
							for _, v in pairs(T) do
								table.insert(ids, v[1])
								count = count + 1
							end
							if table.getn(ids) > 0 then
								redis.call('XDEL', KEYS[1], unpack(ids))
							end
							if table.getn(ids) < 1000 then
								all = 1
								break
							end
							if count >= 100000 then
								break
							end
						end
						return all
						`
				r.garbageCollectorSha1 = redisGarbage.ScriptLoad(script)
				redisGarbage.Set("_orm_gc_sha1", r.garbageCollectorSha1, 604800)
			} else {
				r.garbageCollectorSha1 = sha1
			}
		}

		for {
			res := redisGarbage.EvalSha(r.garbageCollectorSha1, []string{stream}, end)
			if res == int64(1) {
				break
			}
		}
	}
}
