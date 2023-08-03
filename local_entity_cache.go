package beeorm

import (
	"container/list"
	"fmt"
	"reflect"
	"sync"
)

var nilValue reflect.Value

type LocalEntityCache interface {
	Remove(c Context, id uint64)
	Len() int
	Clear(c Context)
	get(c Context, id uint64) (value reflect.Value, ok bool)
	getQuery(c Context, key string) (value []uint64, ok bool)
	add(c Context, id uint64, value reflect.Value)
	addQuery(c Context, key string, value []uint64)
	addNil(c Context, id uint64)
	removeQuery(c Context, key string)
}

type localEntityCache struct {
	schema       EntitySchema
	maxEntries   int
	ll           *list.List
	cache        map[uint64]*list.Element
	maxQueries   int
	llQueries    *list.List
	cacheQueries map[string]*list.Element
	mutex        sync.Mutex
}

type entityCacheSetter struct {
	schema         EntitySchema
	setIds         []uint64
	setValues      []reflect.Value
	removes        []uint64
	removesQueries []string
}

type entry struct {
	id    uint64
	value reflect.Value
}

type entryQuery struct {
	key   string
	value []uint64
}

func newLocalEntityCache(schema EntitySchema, maxEntries, maxQueries int) *localEntityCache {
	return &localEntityCache{
		schema:       schema,
		maxEntries:   maxEntries,
		ll:           list.New(),
		cache:        make(map[uint64]*list.Element),
		maxQueries:   maxQueries,
		llQueries:    list.New(),
		cacheQueries: make(map[string]*list.Element),
	}
}

func (ecs *entityCacheSetter) add(id uint64, value reflect.Value) {
	ecs.setIds = append(ecs.setIds, id)
	ecs.setValues = append(ecs.setValues, value)
}

func (ecs *entityCacheSetter) addNil(id uint64) {
	ecs.setIds = append(ecs.setIds, id)
	ecs.setValues = append(ecs.setValues, nilValue)
}

func (ecs *entityCacheSetter) remove(id ...uint64) {
	ecs.removes = append(ecs.removes, id...)
}

func (ecs *entityCacheSetter) removeQuery(key ...string) {
	ecs.removesQueries = append(ecs.removesQueries, key...)
}

func (ecs *entityCacheSetter) flush(c Context) {
	lc, _ := ecs.schema.GetLocalCache()
	for i, id := range ecs.setIds {
		lc.add(c, id, ecs.setValues[i])
	}
	for _, id := range ecs.removes {
		lc.Remove(c, id)
	}
	for _, key := range ecs.removesQueries {
		lc.removeQuery(c, key)
	}
	ecs.setIds = nil
	ecs.setValues = nil
	ecs.removes = nil
	ecs.removesQueries = nil
}

func (lc *localEntityCache) add(c Context, id uint64, value reflect.Value) {
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET %d %v", id, value), false)
	}
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if ee, ok := lc.cache[id]; ok {
		lc.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return
	}
	ele := lc.ll.PushFront(&entry{id, value})
	lc.cache[id] = ele
	if lc.maxEntries != 0 && lc.ll.Len() > lc.maxEntries {
		lc.removeOldest()
	}
}

func (lc *localEntityCache) addQuery(c Context, key string, value []uint64) {
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "SET", fmt.Sprintf("SET %s %v", key, value), false)
	}
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if ee, ok := lc.cacheQueries[key]; ok {
		lc.llQueries.MoveToFront(ee)
		ee.Value.(*entryQuery).value = value
		return
	}
	ele := lc.llQueries.PushFront(&entryQuery{key, value})
	lc.cacheQueries[key] = ele
	if lc.maxQueries != 0 && lc.llQueries.Len() > lc.maxQueries {
		lc.removeOldestQuery()
	}
}

func (lc *localEntityCache) addNil(c Context, id uint64) {
	lc.add(c, id, nilValue)
}

func (lc *localEntityCache) get(c Context, id uint64) (value reflect.Value, ok bool) {
	hasLog, _ := c.getLocalCacheLoggers()
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if ele, hit := lc.cache[id]; hit {
		lc.ll.MoveToFront(ele)
		if hasLog {
			lc.fillLogFields(c, "GET", fmt.Sprintf("GET %d", id), false)
		}
		return ele.Value.(*entry).value, true
	}
	if hasLog {
		lc.fillLogFields(c, "GET", fmt.Sprintf("GET %d", id), true)
	}
	return
}

func (lc *localEntityCache) getQuery(c Context, key string) (value []uint64, ok bool) {
	hasLog, _ := c.getLocalCacheLoggers()
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if ele, hit := lc.cacheQueries[key]; hit {
		lc.llQueries.MoveToFront(ele)
		if hasLog {
			lc.fillLogFields(c, "GET", fmt.Sprintf("GET %s", key), false)
		}
		return ele.Value.(*entryQuery).value, true
	}
	lc.fillLogFields(c, "GET", fmt.Sprintf("GET %s", key), true)
	return
}

func (lc *localEntityCache) Remove(c Context, id uint64) {
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE %d", id), false)
	}
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if ele, hit := lc.cache[id]; hit {
		lc.removeElement(ele)
	}
}

func (lc *localEntityCache) removeQuery(c Context, key string) {
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "REMOVE", fmt.Sprintf("REMOVE %s", key), false)
	}
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if ele, hit := lc.cacheQueries[key]; hit {
		lc.removeElementQuery(ele)
	}
}

func (lc *localEntityCache) removeElement(e *list.Element) {
	lc.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(lc.cache, kv.id)
}

func (lc *localEntityCache) removeElementQuery(e *list.Element) {
	lc.llQueries.Remove(e)
	kv := e.Value.(*entryQuery)
	delete(lc.cacheQueries, kv.key)
}

func (lc *localEntityCache) removeOldest() {
	ele := lc.ll.Back()
	if ele != nil {
		lc.removeElement(ele)
	}
}

func (lc *localEntityCache) removeOldestQuery() {
	ele := lc.llQueries.Back()
	if ele != nil {
		lc.removeElementQuery(ele)
	}
}

func (lc *localEntityCache) Len() int {
	return lc.ll.Len() + lc.llQueries.Len()
}

func (lc *localEntityCache) Clear(c Context) {
	hasLog, _ := c.getLocalCacheLoggers()
	if hasLog {
		lc.fillLogFields(c, "CLEAR", "CLEAR", false)
	}
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	lc.ll = list.New()
	lc.cache = make(map[uint64]*list.Element)
	lc.llQueries = list.New()
	lc.cacheQueries = make(map[string]*list.Element)
}

func (lc *localEntityCache) fillLogFields(c Context, operation, query string, cacheMiss bool) {
	_, loggers := c.getLocalCacheLoggers()
	fillLogFields(c, loggers, lc.schema.GetEntityName(), sourceEntityCache, operation, query, nil, cacheMiss, nil)
}
