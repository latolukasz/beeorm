package beeorm

import (
	"strconv"
	"strings"
	"time"
)

const StreamGarbageCollectorChannelName = "orm-stream-garbage-collector-stream"
const StreamGarbageCollectorGroupName = "orm-garbage-collector-consumer"

type StreamGarbageCollectorConsumer struct {
	eventConsumerBase
	garbageCollectorSha1 string
	consumer             *eventsConsumer
}

func NewStreamGarbageCollectorConsumer(c Context) *StreamGarbageCollectorConsumer {
	consumer := &StreamGarbageCollectorConsumer{}
	consumer.c = c
	consumer.block = true
	consumer.blockTime = time.Second * 30
	return consumer
}

func (r *StreamGarbageCollectorConsumer) Digest() bool {
	r.consumer = r.c.EventBroker().Consumer(StreamGarbageCollectorGroupName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(500, func(events []Event) {
		for _, e := range events {
			switch e.Stream() {
			case StreamGarbageCollectorChannelName:
				r.handleEvent(e)
			}
		}
	})
}

func (r *StreamGarbageCollectorConsumer) handleEvent(event Event) {
	garbageEvent := &garbageCollectorEvent{}
	event.Unserialize(garbageEvent)
	engine := r.c.Engine()
	redisGarbage := engine.Redis(garbageEvent.Pool).(*redisCache)
	streams := engine.Registry().getRedisStreamsForGroup(garbageEvent.Group)
	if !redisGarbage.SetNX(r.c, garbageEvent.Group+"_gc", "1", 30*time.Second) {
		event.delete(r.c)
		return
	}
	def := engine.Registry().(*engineRegistryImplementation).redisStreamGroups[redisGarbage.GetPoolConfig().GetCode()]
	for _, stream := range streams {
		info := redisGarbage.XInfoGroups(r.c, stream)
		ids := make(map[string][]int64)
		for name := range def[stream] {
			ids[name] = []int64{0, 0}
		}
		inPending := false
		for _, group := range info {
			_, has := ids[group.Name]
			if has && group.LastDeliveredID != "" {
				lastDelivered := group.LastDeliveredID
				pending := redisGarbage.XPending(r.c, stream, group.Name)
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
			r.setGCScript(redisGarbage)
		}

		for {
			res, exists := redisGarbage.EvalSha(r.c, r.garbageCollectorSha1, []string{redisGarbage.addNamespacePrefix(stream)}, end)
			if !exists {
				r.setGCScript(redisGarbage)
				res, _ = redisGarbage.EvalSha(r.c, r.garbageCollectorSha1, []string{redisGarbage.addNamespacePrefix(stream)}, end)
			}
			if res == int64(1) {
				break
			}
		}
	}
	event.delete(r.c)
}

func (r *StreamGarbageCollectorConsumer) setGCScript(redisGarbage RedisCache) {
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
	r.garbageCollectorSha1 = redisGarbage.ScriptLoad(r.c, script)
}
