package beeorm

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type Engine interface {
	Clone() Engine
	EnableRequestCache()
	SetQueryTimeLimit(seconds int)
	GetMysql(code ...string) *DB
	GetLocalCache(code ...string) *LocalCache
	GetRedis(code ...string) *RedisCache
	SetLogMetaData(key string, value interface{})
	NewFlusher() Flusher
	Flush(entity ...Entity)
	FlushLazy(entity ...Entity)
	FlushWithCheck(entity ...Entity) error
	FlushWithFullCheck(entity ...Entity) error
	Delete(entity ...Entity)
	DeleteLazy(entity ...Entity)
	ForceDelete(entity ...Entity)
	GetRegistry() ValidatedRegistry
	SearchWithCount(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int)
	Search(where *Where, pager *Pager, entities interface{}, references ...string)
	SearchIDsWithCount(where *Where, pager *Pager, entity Entity) (results []uint64, totalRows int)
	SearchIDs(where *Where, pager *Pager, entity Entity) []uint64
	SearchOne(where *Where, entity Entity, references ...string) (found bool)
	CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool)
	CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool)
	CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int)
	CachedSearchCount(entity Entity, indexName string, arguments ...interface{}) int
	ClearCacheByIDs(entity Entity, ids ...uint64)
	LoadByID(id uint64, entity Entity, references ...string) (found bool)
	ReadByID(id uint64, schema TableSchema) (entity Entity)
	Load(entity Entity, references ...string) (found bool)
	LoadByIDs(ids []uint64, entities interface{}, references ...string) (found bool)
	ReadByIDs(ids []uint64, entities interface{}, references ...string) (found bool)
	GetAlters() (alters []Alter)
	GetEventBroker() EventBroker
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
}

type engineImplementation struct {
	registry                  *validatedRegistry
	dbs                       map[string]*DB
	localCache                map[string]*LocalCache
	redis                     map[string]*RedisCache
	logMetaData               Bind
	hasRequestCache           bool
	queryLoggersDB            []LogHandler
	queryLoggersRedis         []LogHandler
	queryLoggersLocalCache    []LogHandler
	hasRedisLogger            bool
	hasDBLogger               bool
	hasLocalCacheLogger       bool
	afterCommitLocalCacheSets map[string][]interface{}
	afterCommitRedisFlusher   *redisFlusher
	eventBroker               *eventBroker
	queryTimeLimit            uint16
	serializer                *serializer
	stringBuilder             strings.Builder
	cacheStrings              []string
	sync.Mutex
}

func (e *engineImplementation) Clone() Engine {
	return &engineImplementation{
		registry:               e.registry,
		queryTimeLimit:         e.queryTimeLimit,
		logMetaData:            e.logMetaData,
		hasRequestCache:        e.hasRequestCache,
		queryLoggersDB:         e.queryLoggersDB,
		queryLoggersRedis:      e.queryLoggersRedis,
		queryLoggersLocalCache: e.queryLoggersLocalCache,
		hasRedisLogger:         e.hasRedisLogger,
		hasDBLogger:            e.hasDBLogger,
		hasLocalCacheLogger:    e.hasLocalCacheLogger,
	}
}

func (e *engineImplementation) getCacheKey(schema *tableSchema, id uint64) string {
	builder := e.getStringBuilder()
	builder.WriteString(schema.cachePrefix)
	builder.WriteString(":")
	builder.WriteString(strconv.FormatUint(id, 10))
	return builder.String()
}

func (e *engineImplementation) EnableRequestCache() {
	e.hasRequestCache = true
}

func (e *engineImplementation) SetQueryTimeLimit(seconds int) {
	e.queryTimeLimit = uint16(seconds)
}

func (e *engineImplementation) GetMysql(code ...string) *DB {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	db, has := e.dbs[dbCode]
	if !has {
		config, has := e.registry.mySQLServers[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered mysql pool '%s'", dbCode))
		}
		db = &DB{engine: e, config: config, client: &standardSQLClient{db: config.getClient()}}
		if e.dbs == nil {
			e.dbs = map[string]*DB{dbCode: db}
		} else {
			e.dbs[dbCode] = db
		}
	}
	return db
}

func (e *engineImplementation) GetLocalCache(code ...string) *LocalCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	cache, has := e.localCache[dbCode]
	if !has {
		config, has := e.registry.localCacheServers[dbCode]
		if !has {
			if dbCode == requestCacheKey {
				cache = &LocalCache{config: newLocalCacheConfig(dbCode, 5000), engine: e}
				if e.localCache == nil {
					e.localCache = map[string]*LocalCache{dbCode: cache}
				} else {
					e.localCache[dbCode] = cache
				}
				return cache
			}
			panic(fmt.Errorf("unregistered local cache pool '%s'", dbCode))
		}
		cache = &LocalCache{engine: e, config: config.(*localCachePoolConfig)}
		if e.localCache == nil {
			e.localCache = make(map[string]*LocalCache)
		}
		e.localCache[dbCode] = cache
	}
	return cache
}

func (e *engineImplementation) GetRedis(code ...string) *RedisCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	cache, has := e.redis[dbCode]
	if !has {
		config, has := e.registry.redisServers[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered redis cache pool '%s'", dbCode))
		}
		client := config.getClient()
		cache = &RedisCache{engine: e, config: config, client: client}
		if e.redis == nil {
			e.redis = map[string]*RedisCache{dbCode: cache}
		} else {
			e.redis[dbCode] = cache
		}
	}
	return cache
}

func (e *engineImplementation) SetLogMetaData(key string, value interface{}) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	if e.logMetaData == nil {
		e.logMetaData = make(Bind)
	}
	e.logMetaData[key] = value
}

func (e *engineImplementation) NewFlusher() Flusher {
	return &flusher{engine: e}
}

func (e *engineImplementation) Flush(entity ...Entity) {
	e.NewFlusher().Track(entity...).Flush()
}

func (e *engineImplementation) FlushLazy(entity ...Entity) {
	e.NewFlusher().Track(entity...).FlushLazy()
}

func (e *engineImplementation) FlushWithCheck(entity ...Entity) error {
	return e.NewFlusher().Track(entity...).FlushWithCheck()
}

func (e *engineImplementation) FlushWithFullCheck(entity ...Entity) error {
	return e.NewFlusher().Track(entity...).FlushWithFullCheck()
}

func (e *engineImplementation) Delete(entity ...Entity) {
	for _, e := range entity {
		e.markToDelete()
	}
	e.Flush(entity...)
}

func (e *engineImplementation) DeleteLazy(entity ...Entity) {
	for _, e := range entity {
		e.markToDelete()
	}
	e.FlushLazy(entity...)
}

func (e *engineImplementation) ForceDelete(entity ...Entity) {
	for _, entity := range entity {
		entity.forceMarkToDelete()
	}
	e.Flush(entity...)
}

func (e *engineImplementation) GetRegistry() ValidatedRegistry {
	return e.registry
}

func (e *engineImplementation) SearchWithCount(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int) {
	return search(e.getSerializer(nil), e, where, pager, true, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *engineImplementation) Search(where *Where, pager *Pager, entities interface{}, references ...string) {
	search(e.getSerializer(nil), e, where, pager, false, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *engineImplementation) SearchIDsWithCount(where *Where, pager *Pager, entity Entity) (results []uint64, totalRows int) {
	return searchIDsWithCount(e, where, pager, reflect.TypeOf(entity).Elem())
}

func (e *engineImplementation) SearchIDs(where *Where, pager *Pager, entity Entity) []uint64 {
	results, _ := searchIDs(e, where, pager, false, reflect.TypeOf(entity).Elem())
	return results
}

func (e *engineImplementation) SearchOne(where *Where, entity Entity, references ...string) (found bool) {
	found, _, _ = searchOne(e.getSerializer(nil), e, where, entity, references)
	return found
}

func (e *engineImplementation) CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool) {
	return cachedSearchOne(e.getSerializer(nil), e, entity, indexName, true, arguments, nil)
}

func (e *engineImplementation) CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool) {
	return cachedSearchOne(e.getSerializer(nil), e, entity, indexName, true, arguments, references)
}

func (e *engineImplementation) CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int) {
	return cachedSearch(e.getSerializer(nil), e, entities, indexName, pager, arguments, true)
}

func (e *engineImplementation) CachedSearchCount(entity Entity, indexName string, arguments ...interface{}) int {
	return cachedSearch(e.getSerializer(nil), e, entity, indexName, NewPager(1, 1), arguments, false)
}

func (e *engineImplementation) ClearCacheByIDs(entity Entity, ids ...uint64) {
	clearByIDs(e, entity, ids...)
}

func (e *engineImplementation) getSerializer(buf []uint8) *serializer {
	if e.serializer == nil {
		e.serializer = &serializer{buffer: bytes.NewBuffer(buf)}
	} else {
		e.serializer.buffer.Reset()
		if buf != nil {
			e.serializer.buffer.Write(buf)
		}
	}
	return e.serializer
}

func (e *engineImplementation) getStringBuilder() strings.Builder {
	e.stringBuilder.Reset()
	return e.stringBuilder
}

func (e *engineImplementation) getCacheStrings(length, capacity int) []string {
	if e.cacheStrings == nil || cap(e.cacheStrings) < capacity {
		e.cacheStrings = make([]string, length, capacity)
		return e.cacheStrings
	}
	return e.cacheStrings[0:length]
}

func (e *engineImplementation) LoadByID(id uint64, entity Entity, references ...string) (found bool) {
	found, _, _ = loadByID(e.getSerializer(nil), e, id, entity, nil, true, references...)
	return found
}

func (e *engineImplementation) ReadByID(id uint64, schema TableSchema) (entity Entity) {
	found, entity, _ := loadByID(e.getSerializer(nil), e, id, nil, schema.(*tableSchema), true)
	if !found {
		return nil
	}
	return entity
}

func (e *engineImplementation) Load(entity Entity, references ...string) (found bool) {
	return e.load(e.getSerializer(nil), entity, references...)
}

func (e *engineImplementation) LoadByIDs(ids []uint64, entities interface{}, references ...string) (found bool) {
	_, hasMissing := tryByIDs(e.getSerializer(nil), e, ids, reflect.ValueOf(entities), references, false)
	return !hasMissing
}

func (e *engineImplementation) ReadByIDs(ids []uint64, entities interface{}, references ...string) (found bool) {
	_, hasMissing := tryByIDs(e.getSerializer(nil), e, ids, reflect.ValueOf(entities), references, true)
	return !hasMissing
}

func (e *engineImplementation) GetAlters() (alters []Alter) {
	return getAlters(e)
}

func (e *engineImplementation) load(serializer *serializer, entity Entity, references ...string) bool {
	if entity.IsLoaded() {
		if len(references) > 0 {
			orm := entity.getORM()
			warmUpReferences(serializer, e, orm.tableSchema, orm.elem, references, false)
		}
		return true
	}
	orm := initIfNeeded(e.registry, entity)
	id := orm.GetID()
	found := false
	if id > 0 {
		found, _, _ = loadByID(serializer, e, id, entity, nil, true, references...)
	}
	return found
}
