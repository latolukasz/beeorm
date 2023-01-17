package beeorm

import (
	"fmt"
	"reflect"
	"sync"
)

type Engine interface {
	Clone() Engine
	EnableRequestCache()
	SetQueryTimeLimit(seconds int)
	GetMysql(code ...string) *DB
	GetLocalCache(code ...string) LocalCache
	GetRedis(code ...string) RedisCache
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
	SetOption(plugin, key string, value interface{})
	GetOption(plugin, key string) interface{}
}

type engineImplementation struct {
	registry               *validatedRegistry
	dbs                    map[string]*DB
	localCache             map[string]*localCache
	redis                  map[string]*redisCache
	hasRequestCache        bool
	queryLoggersDB         []LogHandler
	queryLoggersRedis      []LogHandler
	queryLoggersLocalCache []LogHandler
	hasRedisLogger         bool
	hasDBLogger            bool
	hasLocalCacheLogger    bool
	afterCommit            func()
	eventBroker            *eventBroker
	queryTimeLimit         uint16
	options                map[string]map[string]interface{}
	sync.Mutex
}

func (e *engineImplementation) Clone() Engine {
	return &engineImplementation{
		registry:               e.registry,
		hasRequestCache:        e.hasRequestCache,
		queryLoggersDB:         e.queryLoggersDB,
		queryLoggersRedis:      e.queryLoggersRedis,
		queryLoggersLocalCache: e.queryLoggersLocalCache,
		hasRedisLogger:         e.hasRedisLogger,
		hasDBLogger:            e.hasDBLogger,
		hasLocalCacheLogger:    e.hasLocalCacheLogger,
	}
}

func (e *engineImplementation) SetOption(plugin, key string, value interface{}) {
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

func (e *engineImplementation) GetOption(plugin, key string) interface{} {
	if e.options == nil {
		return nil
	}
	values, has := e.options[plugin]
	if !has {
		return nil
	}
	return values[key]
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
			if dbCode == requestCacheKey {
				cache = &localCache{config: newLocalCacheConfig(dbCode, 5000), engine: e}
				if e.localCache == nil {
					e.localCache = map[string]*localCache{dbCode: cache}
				} else {
					e.localCache[dbCode] = cache
				}
				return cache
			}
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
	return search(newSerializer(nil), e, where, pager, true, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *engineImplementation) Search(where *Where, pager *Pager, entities interface{}, references ...string) {
	search(newSerializer(nil), e, where, pager, false, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *engineImplementation) SearchIDsWithCount(where *Where, pager *Pager, entity Entity) (results []uint64, totalRows int) {
	return searchIDsWithCount(e, where, pager, reflect.TypeOf(entity).Elem())
}

func (e *engineImplementation) SearchIDs(where *Where, pager *Pager, entity Entity) []uint64 {
	results, _ := searchIDs(e, where, pager, false, reflect.TypeOf(entity).Elem())
	return results
}

func (e *engineImplementation) SearchOne(where *Where, entity Entity, references ...string) (found bool) {
	found, _, _ = searchOne(newSerializer(nil), e, where, entity, references)
	return found
}

func (e *engineImplementation) CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool) {
	return cachedSearchOne(newSerializer(nil), e, entity, indexName, true, arguments, nil)
}

func (e *engineImplementation) CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool) {
	return cachedSearchOne(newSerializer(nil), e, entity, indexName, true, arguments, references)
}

func (e *engineImplementation) CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int) {
	total, _ := cachedSearch(newSerializer(nil), e, entities, indexName, pager, arguments, true, nil)
	return total
}

func (e *engineImplementation) CachedSearchIDs(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, ids []uint64) {
	return cachedSearch(newSerializer(nil), e, entity, indexName, pager, arguments, false, nil)
}

func (e *engineImplementation) CachedSearchCount(entity Entity, indexName string, arguments ...interface{}) int {
	total, _ := cachedSearch(newSerializer(nil), e, entity, indexName, NewPager(1, 1), arguments, false, nil)
	return total
}

func (e *engineImplementation) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int) {
	total, _ := cachedSearch(newSerializer(nil), e, entities, indexName, pager, arguments, true, references)
	return total
}

func (e *engineImplementation) ClearCacheByIDs(entity Entity, ids ...uint64) {
	clearByIDs(e, entity, ids...)
}

func (e *engineImplementation) LoadByID(id uint64, entity Entity, references ...string) (found bool) {
	found, _ = loadByID(newSerializer(nil), e, id, entity, true, references...)
	return found
}

func (e *engineImplementation) Load(entity Entity, references ...string) (found bool) {
	return e.load(newSerializer(nil), entity, references...)
}

func (e *engineImplementation) LoadByIDs(ids []uint64, entities interface{}, references ...string) (found bool) {
	_, hasMissing := tryByIDs(newSerializer(nil), e, ids, reflect.ValueOf(entities).Elem(), references)
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
		found, _ = loadByID(serializer, e, id, entity, true, references...)
	}
	return found
}
