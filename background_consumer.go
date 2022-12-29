package beeorm

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/shamaton/msgpack"

	jsoniter "github.com/json-iterator/go"
)

const LazyChannelName = "orm-lazy-channel"
const LogChannelName = "orm-log-channel"
const RedisStreamGarbageCollectorChannelName = "orm-stream-garbage-collector"
const BackgroundConsumerGroupName = "orm-async-consumer"

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
	redisFlusher                 *redisFlusher
	garbageCollectorSha1         string
	consumer                     *eventsConsumer
	lazyFlushModulo              uint64
	lazyErrorLock                sync.Mutex
	lazyFlushQueryErrorResolvers []LazyFlushQueryErrorResolver
}

func NewBackgroundConsumer(engine Engine) *BackgroundConsumer {
	c := &BackgroundConsumer{redisFlusher: &redisFlusher{engine: engine.(*engineImplementation)}}
	c.engine = engine.(*engineImplementation)
	c.block = true
	c.blockTime = time.Second * 30
	c.lazyFlushModulo = 11
	return c
}

type LazyFlushQueryErrorResolver func(engine Engine, db *DB, sql string, queryError *mysql.MySQLError) error

func (r *BackgroundConsumer) RegisterLazyFlushQueryErrorResolver(resolver LazyFlushQueryErrorResolver) {
	r.lazyFlushQueryErrorResolvers = append(r.lazyFlushQueryErrorResolvers, resolver)
}

func (r *BackgroundConsumer) GetLazyFlushEventsSample(count int64) []string {
	sample := make([]string, 0)
	entries := r.engine.GetRedis().XRange(LazyChannelName, "-", "+", count)
	for _, entry := range entries {
		val, has := entry.Values["s"]
		if !has {
			continue
		}
		var data map[interface{}]interface{}
		err := msgpack.Unmarshal([]byte(val.(string)), &data)
		if err != nil {
			continue
		}
		query, hasQuery := data["q"]
		if !hasQuery {
			continue
		}
		queryData, ok := query.([]interface{})
		if !ok || len(queryData) == 0 {
			continue
		}
		queryDetails, ok := queryData[0].([]interface{})
		if !ok || len(queryDetails) < 2 {
			continue
		}
		sample = append(sample, queryDetails[1].(string))
	}
	return sample
}

func (r *BackgroundConsumer) Digest(ctx context.Context) bool {
	r.consumer = r.engine.GetEventBroker().Consumer(BackgroundConsumerGroupName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(ctx, 500, func(events []Event) {
		lazyEvents := make([]Event, 0)
		lazyEventsData := make([]map[string]interface{}, 0)
		logEventsData := make(map[string][]*LogQueueValue)
		var lazyError error
		for _, event := range events {
			switch event.Stream() {
			case LazyChannelName:
				lazyEvents = append(lazyEvents, event)
				var data map[string]interface{}
				event.Unserialize(&data)
				lazyEventsData = append(lazyEventsData, data)
			case LogChannelName:
				var data LogQueueValue
				event.Unserialize(&data)
				_, has := logEventsData[data.PoolName]
				if !has {
					logEventsData[data.PoolName] = make([]*LogQueueValue, 0)
				}
				logEventsData[data.PoolName] = append(logEventsData[data.PoolName], &data)
			case RedisStreamGarbageCollectorChannelName:
				r.handleRedisChannelGarbageCollector(event)
			}
		}
		l := len(lazyEvents)
		if l > 0 {
			insertEvents := make(map[string][]int)
			groupQueries := make(map[string]map[int]string)
			groupEvents := make(map[string]map[int][]int)
		MAIN:
			for i, data := range lazyEventsData {
				queries, has := data["q"]
				ids, hasIDs := data["i"]
				if has {
					validQueries := queries.([]interface{})
					for k, query := range validQueries {
						validInsert := query.([]interface{})
						code := validInsert[0].(string)
						sql := validInsert[1].(string)
						operation := data["o"]
						isInsert := operation == "i"
						if isInsert {
							insertEvents[code] = append(insertEvents[code], i)
							continue MAIN
						}
						id := uint64(0)
						if hasIDs {
							id, _ = strconv.ParseUint(fmt.Sprintf("%v", ids.([]interface{})[k]), 10, 64)
						}
						modulo := int(id % r.lazyFlushModulo)
						before := groupQueries[code][modulo]
						before += sql + ";"
						if groupQueries[code] == nil {
							groupQueries[code] = make(map[int]string)
							groupEvents[code] = make(map[int][]int)
						}
						groupQueries[code][modulo] = before
						groupEvents[code][modulo] = append(groupEvents[code][modulo], i)
					}
				} else {
					r.handleLazy(lazyEvents[i], data)
				}
			}
			if len(insertEvents) > 0 {
				wg := &sync.WaitGroup{}
				for _, insertGroup := range insertEvents {
					wg.Add(1)
					ids := insertGroup
					go func() {
						defer wg.Done()
						defer func() {
							if rec := recover(); rec != nil {
								assErr, is := rec.(error)
								if !is {
									assErr = fmt.Errorf(fmt.Sprintf("%v", rec))
								}
								r.lazyErrorLock.Lock()
								defer r.lazyErrorLock.Unlock()
								lazyError = assErr
							}
						}()
						for _, i := range ids {
							r.handleLazy(lazyEvents[i], lazyEventsData[i])
						}
					}()
				}
				wg.Wait()
				if lazyError != nil {
					panic(lazyError)
				}
			}
			if len(groupQueries) > 0 {
				wg := &sync.WaitGroup{}
				for code, group := range groupQueries {
					for i, sql := range group {
						key := i
						updateSQL := sql
						dbCode := code
						wg.Add(1)
						go func() {
							defer wg.Done()
							defer func() {
								if rec := recover(); rec != nil {
									assErr, is := rec.(error)
									if !is {
										assErr = fmt.Errorf(fmt.Sprintf("%v", rec))
									}
									r.lazyErrorLock.Lock()
									defer r.lazyErrorLock.Unlock()
									lazyError = assErr
								}
							}()
							if len(groupEvents[dbCode][key]) == 1 {
								r.engine.GetMysql(dbCode).Exec(updateSQL)
							} else {
								deadlock := false
								func() {
									defer func() {
										if rec := recover(); rec != nil {
											asErr, is := rec.(error)
											if is && strings.Contains(asErr.Error(), "Deadlock found") {
												deadlock = true
											} else {
												panic(rec)
											}
										}
									}()
									db := r.engine.Clone().GetMysql(dbCode)
									db.Begin()
									defer db.Rollback()
									db.Exec(updateSQL)
									db.Commit()
								}()
								if deadlock {
									time.Sleep(time.Millisecond * 30)
									log.Printf("DEADLOCK FOUND\n%s\n", updateSQL)
									func() {
										db := r.engine.Clone().GetMysql(dbCode)
										db.Begin()
										defer db.Rollback()
										db.Exec(updateSQL)
										db.Commit()
									}()
								}
							}
							for _, k := range groupEvents[dbCode][key] {
								lazyEvents[k].Ack()
								r.handleCache(lazyEventsData[k], nil)
							}
						}()
					}
				}
				wg.Wait()
				if lazyError != nil {
					panic(lazyError)
				}
			}
		}
		r.handleLog(logEventsData)
	})
}

func (r *BackgroundConsumer) handleLog(values map[string][]*LogQueueValue) {
	for poolName, rows := range values {
		poolDB := r.engine.GetMysql(poolName)
		query := ""
		for _, value := range rows {
			/* #nosec */
			query += "INSERT INTO `" + value.TableName + "`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(" +
				strconv.FormatUint(value.ID, 10) + ",'" + value.Updated.Format(timeFormat) + "',"
			var meta, before, changes string
			if value.Meta != nil {
				meta, _ = jsoniter.ConfigFastest.MarshalToString(value.Meta)
				query += escapeSQLString(meta) + ","
			} else {
				query += "NULL,"
			}
			if value.Before != nil {
				before, _ = jsoniter.ConfigFastest.MarshalToString(value.Before)
				query += escapeSQLString(before) + ","
			} else {
				query += "NULL,"
			}
			if value.Changes != nil {
				changes, _ = jsoniter.ConfigFastest.MarshalToString(value.Changes)
				query += escapeSQLString(changes)
			} else {
				query += "NULL"
			}
			query += ");"
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
					if isMySQLError && asMySQLError.Number == 1146 { // table was removed
						return
					}
					panic(rec)
				}
			}()
			if len(rows) > 1 {
				func() {
					poolDB.Begin()
					defer poolDB.Rollback()
					poolDB.Exec(query)
					poolDB.Commit()
				}()
			} else {
				poolDB.Exec(query)
			}
		}()
	}
}

func (r *BackgroundConsumer) handleLazy(event Event, data map[string]interface{}) {
	ids, err := r.handleQueries(r.engine, data)
	if err != nil {
		panic(err)
	}
	r.handleCache(data, ids)
	event.Ack()
}

func (r *BackgroundConsumer) handleQueries(engine *engineImplementation, validMap map[string]interface{}) ([]uint64, error) {
	queries, has := validMap["q"]
	var ids []uint64
	if has {
		validQueries := queries.([]interface{})
		ids = make([]uint64, len(validQueries))
	MAIN:
		for i, query := range validQueries {
			validInsert := query.([]interface{})
			code := validInsert[0].(string)
			db := engine.GetMysql(code)
			sql := validInsert[1].(string)
			res, err := db.exec(sql)
			if err != nil {
				for _, resolver := range r.lazyFlushQueryErrorResolvers {
					resolverError := resolver(r.engine, db, sql, err.(*mysql.MySQLError))
					if resolverError == nil {
						continue MAIN
					}
				}
				return nil, err
			}
			operation := validMap["o"]
			isInsert := operation == "i"
			if isInsert {
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
			}
		}
	}
	return ids, nil
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
			r.handleLog(map[string][]*LogQueueValue{logEvent.PoolName: {logEvent}})
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
			r.setGCScript(redisGarbage)
		}

		for {
			res, exists := redisGarbage.EvalSha(r.garbageCollectorSha1, []string{redisGarbage.addNamespacePrefix(stream)}, end)
			if !exists {
				r.setGCScript(redisGarbage)
				res, _ = redisGarbage.EvalSha(r.garbageCollectorSha1, []string{redisGarbage.addNamespacePrefix(stream)}, end)
			}
			if res == int64(1) {
				break
			}
		}
	}
	event.delete()
}

func (r *BackgroundConsumer) setGCScript(redisGarbage *RedisCache) {
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
