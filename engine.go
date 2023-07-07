package beeorm

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
)

type Engine interface {
	Clone() Engine
	GetMysql(code ...string) *DB
	GetLocalCache(code ...string) LocalCache
	GetRedis(code ...string) RedisCache
	IsDirty(entity Entity) bool
	GetDirtyBind(entity Entity) (bind Bind, has bool)
	NewFlusher() Flusher
	Flush(entity ...Entity)
	FlushLazy(entity ...Entity)
	FlushWithCheck(entity ...Entity) error
	FlushWithFullCheck(entity ...Entity) error
	Delete(entity ...Entity)
	DeleteLazy(entity ...Entity)
	GetRegistry() ValidatedRegistry
	SearchWithCount(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int)
	Search(where *Where, pager *Pager, entities interface{}, references ...string)
	SearchIDsWithCount(where *Where, pager *Pager, entity Entity) (results []uint64, totalRows int)
	SearchIDs(where *Where, pager *Pager, entity Entity) []uint64
	SearchOne(where *Where, entity Entity, references ...string) (found bool)
	CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool)
	CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool)
	CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int)
	CachedSearchIDs(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, ids []uint64)
	CachedSearchCount(entity Entity, indexName string, arguments ...interface{}) int
	CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager, arguments []interface{}, references []string) (totalRows int)
	ClearCacheByIDs(entity Entity, ids ...uint64)
	LoadByID(id uint64, entity Entity, references ...string) (found bool)
	Load(entity Entity, references ...string) (found bool)
	LoadByIDs(ids []uint64, entities interface{}, references ...string) (found bool)
	GetAlters() (alters []Alter)
	GetEventBroker() EventBroker
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetPluginOption(plugin, key string, value interface{})
	GetPluginOption(plugin, key string) interface{}
	SetMetaData(key, value string)
	GetMetaData() Meta
	HasRedisLogger() (bool, []LogHandler)
}

type engineImplementation struct {
	registry               *validatedRegistry
	dbs                    map[string]*DB
	localCache             map[string]*localCache
	redis                  map[string]*redisCache
	queryLoggersDB         []LogHandler
	queryLoggersRedis      []LogHandler
	queryLoggersLocalCache []LogHandler
	hasRedisLogger         bool
	hasDBLogger            bool
	hasLocalCacheLogger    bool
	eventBroker            *eventBroker
	options                map[string]map[string]interface{}
	meta                   Meta
	serializer             *serializer
	sync.Mutex
}

func (e *engineImplementation) Clone() Engine {
	return &engineImplementation{
		registry:               e.registry,
		queryLoggersDB:         e.queryLoggersDB,
		queryLoggersRedis:      e.queryLoggersRedis,
		queryLoggersLocalCache: e.queryLoggersLocalCache,
		hasRedisLogger:         e.hasRedisLogger,
		hasDBLogger:            e.hasDBLogger,
		hasLocalCacheLogger:    e.hasLocalCacheLogger,
		meta:                   e.meta,
		options:                e.options,
	}
}

func (e *engineImplementation) SetPluginOption(plugin, key string, value interface{}) {
	if e.options == nil {
		e.options = map[string]map[string]interface{}{plugin: {key: value}}
	} else {
		before, has := e.options[plugin]
		if !has {
			e.options[plugin] = map[string]interface{}{key: value}
		} else {
			before[key] = value
		}
	}
}

func (e *engineImplementation) GetPluginOption(plugin, key string) interface{} {
	if e.options == nil {
		return nil
	}
	values, has := e.options[plugin]
	if !has {
		return nil
	}
	return values[key]
}

func (e *engineImplementation) SetMetaData(key, value string) {
	if e.meta == nil {
		e.meta = Meta{key: value}
		return
	}
	e.meta[key] = value
}

func (e *engineImplementation) GetMetaData() Meta {
	return e.meta
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

func (e *engineImplementation) GetLocalCache(code ...string) LocalCache {
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
			panic(fmt.Errorf("unregistered local cache pool '%s'", dbCode))
		}
		cache = &localCache{engine: e, config: config.(*localCachePoolConfig)}
		if e.localCache == nil {
			e.localCache = make(map[string]*localCache)
		}
		e.localCache[dbCode] = cache
	}
	return cache
}

func (e *engineImplementation) GetRedis(code ...string) RedisCache {
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
		cache = &redisCache{engine: e, config: config, client: client}
		if e.redis == nil {
			e.redis = map[string]*redisCache{dbCode: cache}
		} else {
			e.redis[dbCode] = cache
		}
	}
	return cache
}

func (e *engineImplementation) IsDirty(entity Entity) bool {
	orm := initIfNeeded(e.registry, entity)
	if !orm.inDB {
		return true
	}
	_, is := orm.buildDirtyBind(e.getSerializer(nil), false)
	return is
}

func (e *engineImplementation) GetDirtyBind(entity Entity) (bind Bind, has bool) {
	orm := initIfNeeded(e.registry, entity)
	bindBuilder, has := orm.buildDirtyBind(e.getSerializer(nil), false)
	return bindBuilder.Update, has
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
	total, _ := cachedSearch(e.getSerializer(nil), e, entities, indexName, pager, arguments, true, nil)
	return total
}

func (e *engineImplementation) CachedSearchIDs(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, ids []uint64) {
	return cachedSearch(e.getSerializer(nil), e, entity, indexName, pager, arguments, false, nil)
}

func (e *engineImplementation) CachedSearchCount(entity Entity, indexName string, arguments ...interface{}) int {
	total, _ := cachedSearch(e.getSerializer(nil), e, entity, indexName, NewPager(1, 1), arguments, false, nil)
	return total
}

func (e *engineImplementation) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int) {
	total, _ := cachedSearch(e.getSerializer(nil), e, entities, indexName, pager, arguments, true, references)
	return total
}

func (e *engineImplementation) ClearCacheByIDs(entity Entity, ids ...uint64) {
	clearByIDs(e, entity, ids...)
}

func (e *engineImplementation) LoadByID(id uint64, entity Entity, references ...string) (found bool) {
	found, _ = loadByID(e.getSerializer(nil), e, id, entity, true, references...)
	return found
}

func (e *engineImplementation) Load(entity Entity, references ...string) (found bool) {
	return e.load(e.getSerializer(nil), entity, references...)
}

func (e *engineImplementation) LoadByIDs(ids []uint64, entities interface{}, references ...string) (found bool) {
	_, hasMissing := tryByIDs(e.getSerializer(nil), e, ids, reflect.ValueOf(entities), references)
	return !hasMissing
}

func (e *engineImplementation) GetAlters() (alters []Alter) {
	pre, alters, post := getAlters(e)
	final := pre
	final = append(final, alters...)
	final = append(final, post...)
	return final
}

func (e *engineImplementation) HasRedisLogger() (bool, []LogHandler) {
	if e.hasRedisLogger {
		return true, e.queryLoggersRedis
	}
	return false, nil
}

func (e *engineImplementation) load(serializer *serializer, entity Entity, references ...string) bool {
	if entity.IsLoaded() {
		if len(references) > 0 {
			orm := entity.getORM()
			warmUpReferences(serializer, e, orm.entitySchema, orm.elem, references, false)
		}
		return true
	}
	orm := initIfNeeded(e.registry, entity)
	found := false
	id := orm.GetID()
	if id > 0 {
		found, _ = loadByID(serializer, e, id, entity, true, references...)
	}
	return found
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
