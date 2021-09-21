package beeorm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/pkg/errors"
)

const lazyChannelName = "orm-lazy-channel"
const logChannelName = "orm-log-channel"
const redisSearchIndexerChannelName = "orm-redis-search-channel"
const redisStreamGarbageCollectorChannelName = "orm-stream-garbage-collector"
const asyncConsumerGroupName = "orm-async-consumer"

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	LogID     uint64
	Meta      map[string]interface{}
	Before    map[string]interface{}
	Changes   map[string]interface{}
	Updated   time.Time
}

type dirtyQueueValue struct {
	Event   *dirtyEvent
	Streams []string
}

type BackgroundConsumer struct {
	eventConsumerBase
	redisFlusher         *redisFlusher
	garbageCollectorSha1 string
	consumer             *eventsConsumer
}

func NewBackgroundConsumer(engine *Engine) *BackgroundConsumer {
	c := &BackgroundConsumer{redisFlusher: &redisFlusher{engine: engine}}
	c.engine = engine
	c.loop = true
	c.blockTime = time.Second * 30
	return c
}

func (r *BackgroundConsumer) Digest(ctx context.Context) bool {
	r.consumer = r.engine.GetEventBroker().Consumer(asyncConsumerGroupName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(ctx, 100, func(events []Event) {
		for _, event := range events {
			switch event.Stream() {
			case lazyChannelName:
				r.handleLazy(event)
			case logChannelName:
				r.handleLogEvent(event)
			case redisSearchIndexerChannelName:
				r.handleRedisIndexerEvent(event)
			case redisStreamGarbageCollectorChannelName:
				r.handleRedisChannelGarbageCollector(event)
			}
		}
	})
}

func (r *BackgroundConsumer) handleLogEvent(event Event) {
	var value LogQueueValue
	event.Unserialize(&value)
	r.handleLog(&value)
	event.Ack()
}

func (r *BackgroundConsumer) handleLog(value *LogQueueValue) {
	poolDB := r.engine.GetMysql(value.PoolName)
	/* #nosec */
	query := "INSERT INTO `" + value.TableName + "`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)"
	var meta, before, changes interface{}
	if value.Meta != nil {
		meta, _ = jsoniter.ConfigFastest.Marshal(value.Meta)
	}
	if value.Before != nil {
		before, _ = jsoniter.ConfigFastest.Marshal(value.Before)
	}
	if value.Changes != nil {
		changes, _ = jsoniter.ConfigFastest.Marshal(value.Changes)
	}
	poolDB.Exec(query, value.ID, value.Updated.Format(timeFormat), meta, before, changes)
}

func (r *BackgroundConsumer) handleLazy(event Event) {
	var data map[string]interface{}
	event.Unserialize(&data)
	ids := r.handleQueries(r.engine, data)
	r.handleCache(data, ids)
	event.Ack()
}

func (r *BackgroundConsumer) handleQueries(engine *Engine, validMap map[string]interface{}) []uint64 {
	queries, has := validMap["q"]
	var ids []uint64
	if has {
		validQueries := queries.([]interface{})
		ids = make([]uint64, len(validQueries))
		for i, query := range validQueries {
			validInsert := query.([]interface{})
			code := validInsert[0].(string)
			db := engine.GetMysql(code)
			sql := validInsert[1].(string)
			res := db.Exec(sql)
			if sql[0:11] == "INSERT INTO" {
				id := res.LastInsertId()
				ids[i] = res.LastInsertId()
				logEvents, has := validMap["l"]
				if has {
					for _, row := range logEvents.([]interface{}) {
						row.(map[interface{}]interface{})["ID"] = id
						id += db.GetPoolConfig().getAutoincrement()
					}
				}
				dirtyEvents, has := validMap["d"]
				if has {
					for _, row := range dirtyEvents.([]interface{}) {
						row.(map[interface{}]interface{})["Event"].(map[interface{}]interface{})["I"] = id
						id += db.GetPoolConfig().getAutoincrement()
					}
				}
			} else {
				ids[i] = 0
			}
		}
	}
	logEvents, has := validMap["l"]
	if has {
		for _, row := range logEvents.([]interface{}) {
			logEvent := &LogQueueValue{}
			asMap := row.(map[interface{}]interface{})
			logEvent.ID, _ = strconv.ParseUint(fmt.Sprintf("%v", asMap["ID"]), 10, 64)
			logEvent.PoolName = asMap["PoolName"].(string)
			logEvent.TableName = asMap["TableName"].(string)
			logEvent.Updated = time.Now()
			if asMap["Meta"] != nil {
				logEvent.Meta = r.convertMap(asMap["Meta"].(map[interface{}]interface{}))
			}
			if asMap["Before"] != nil {
				logEvent.Before = r.convertMap(asMap["Before"].(map[interface{}]interface{}))
			}
			if asMap["Changes"] != nil {
				logEvent.Changes = r.convertMap(asMap["Changes"].(map[interface{}]interface{}))
			}
			r.handleLog(logEvent)
		}
	}
	dirtyEvents, has := validMap["d"]
	if has {
		for _, row := range dirtyEvents.([]interface{}) {
			asMap := row.(map[interface{}]interface{})
			e := asMap["Event"].(map[interface{}]interface{})
			for _, stream := range asMap["Streams"].([]interface{}) {
				r.redisFlusher.Publish(stream.(string), e)
			}
		}
		r.redisFlusher.Flush()
	}
	return ids
}

func (r *BackgroundConsumer) convertMap(value map[interface{}]interface{}) map[string]interface{} {
	newMap := make(map[string]interface{}, len(value))
	for k, v := range value {
		newMap[k.(string)] = v
	}
	return newMap
}

func (r *BackgroundConsumer) handleCache(validMap map[string]interface{}, ids []uint64) {
	keys, has := validMap["cr"]
	if has {
		idKey := 0
		validKeys := keys.(map[interface{}]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				parts := strings.Split(v.(string), ":")
				l := len(parts)
				if l > 1 {
					if parts[l-1] == "0" {
						parts[l-1] = strconv.FormatUint(ids[idKey], 10)
					}
					idKey++
				}
				stringKeys[i] = strings.Join(parts, ":")
			}
			cache := r.engine.GetRedis(cacheCode.(string))
			cache.Del(stringKeys...)
		}
	}
	localCache, has := validMap["cl"]
	if has {
		validKeys := localCache.(map[interface{}]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				stringKeys[i] = v.(string)
			}
			r.engine.GetLocalCache(cacheCode.(string)).Remove(stringKeys...)
		}
	}
}

func (r *BackgroundConsumer) handleRedisIndexerEvent(event Event) {
	indexEvent := &redisIndexerEvent{}
	event.Unserialize(indexEvent)
	var indexDefinition *RedisSearchIndex
	redisPool := ""
	for pool, list := range r.engine.registry.redisSearchIndexes {
		val, has := list[indexEvent.Index]
		if has {
			indexDefinition = val
			redisPool = pool
			break
		}
	}
	if indexDefinition == nil {
		event.Ack()
		return
	}
	search := r.engine.GetRedisSearch(redisPool)
	pusher := &redisSearchIndexPusher{pipeline: search.redis.PipeLine()}
	id := uint64(0)
	idRedisKey := redisSearchForceIndexLastIDKeyPrefix + indexEvent.Index
	idInRedis, has := search.redis.Get(idRedisKey)
	if has {
		id, _ = strconv.ParseUint(idInRedis, 10, 64)
	}
	for {
		hasMore := false
		nextID := uint64(0)
		if indexDefinition.Indexer != nil {
			newID, hasNext := indexDefinition.Indexer(r.engine, id, pusher)
			hasMore = hasNext
			nextID = newID
			if pusher.pipeline.commands > 0 {
				pusher.Flush()
			}
			if hasMore {
				search.redis.Set(idRedisKey, strconv.FormatUint(nextID, 10), 86400)
			}
		}

		if !hasMore {
			search.redis.Del(idRedisKey)
			break
		}
		if nextID <= id {
			panic(errors.Errorf("loop detected in indexer for index %s in pool %s", indexDefinition.Name, redisPool))
		}
		id = nextID
	}
}

func (r *BackgroundConsumer) handleRedisChannelGarbageCollector(event Event) {
	garbageEvent := &garbageCollectorEvent{}
	event.Unserialize(garbageEvent)
	engine := r.engine
	redisGarbage := engine.GetRedis(garbageEvent.Pool)
	streams := engine.registry.getRedisStreamsForGroup(garbageEvent.Group)
	if !redisGarbage.SetNX(garbageEvent.Group+"_gc", "1", 30) {
		event.delete()
		return
	}
	def := engine.registry.redisStreamGroups[redisGarbage.config.GetCode()]
	for _, stream := range streams {
		info := redisGarbage.XInfoGroups(stream)
		ids := make(map[string][]int64)
		for name := range def[stream] {
			ids[name] = []int64{0, 0}
		}
		inPending := false
		for _, group := range info {
			_, has := ids[group.Name]
			if has && group.LastDeliveredID != "" {
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
		}

		for {
			res := redisGarbage.EvalSha(r.garbageCollectorSha1, []string{redisGarbage.addNamespacePrefix(stream)}, end)
			if res == int64(1) {
				break
			}
		}
	}
	event.delete()
}
