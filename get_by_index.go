package beeorm

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"slices"
	"strconv"

	jsoniter "github.com/json-iterator/go"
)

type indexDefinition struct {
	Cached     bool
	Columns    []string
	Where      string
	Duplicated bool
}

func (d indexDefinition) CreteWhere(hasNil bool, attributes []any) Where {
	if !hasNil {
		return NewWhere(d.Where, attributes...)
	}
	query := ""
	newAttributes := make([]any, 0)
	for i, column := range d.Columns {
		if i > 0 {
			query += " AND "
		}
		if attributes[i] == nil {
			query += "`" + column + "` IS NULL"
		} else {
			query += "`" + column + "`=?"
			newAttributes = append(newAttributes, attributes[i])
		}
	}
	return NewWhere(query, newAttributes)
}

func GetByIndex[E any](orm ORM, indexName string, attributes ...any) EntityIterator[E] {
	var e E
	schema := orm.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	def, has := schema.indexes[indexName]
	if !has {
		panic(fmt.Errorf("unknow index name `%s`", indexName))
	}
	if len(attributes) != len(def.Columns) {
		panic(fmt.Errorf("invalid attributes length, %d is required, %d provided", len(def.Columns), len(attributes)))
	}
	hasNil := false
	for i, attribute := range attributes {
		if attribute == nil {
			hasNil = true
			continue
		}
		setter := schema.fieldBindSetters[def.Columns[i]]
		bind, err := setter(attribute)
		if err != nil {
			panic(err)
		}
		attributes[i] = bind
	}
	if !def.Cached {
		return Search[E](orm, def.CreteWhere(hasNil, attributes), nil)
	}
	return getCachedByColumns[E](orm, indexName, def, schema, attributes, hasNil)
}

func getCachedByColumns[E any](orm ORM, indexName string, index indexDefinition, schema *entitySchema, attributes []any, hasNil bool) EntityIterator[E] {
	bindID := hashIndexAttributes(attributes)
	if schema.hasLocalCache {
		fromCache, hasInCache := schema.localCache.getList(orm, indexName, bindID)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}
			}

			if schema.hasLocalCache {
				results := &entityIterator[E]{index: -1}
				results.rows = fromCache.([]*E)
				return results
			}
			return GetByIDs[E](orm, fromCache.([]uint64)...)
		}
	}
	rc := orm.Engine().Redis(schema.getForcedRedisCode())
	redisSetKey := schema.cacheKey + ":" + indexName + ":" + strconv.FormatUint(bindID, 10)
	fromRedis := rc.SMembers(orm, redisSetKey)
	if len(fromRedis) > 0 {
		ids := make([]uint64, len(fromRedis))
		k := 0
		hasValidValue := false
		for _, value := range fromRedis {
			if value == redisValidSetValue {
				hasValidValue = true
				continue
			} else if value == cacheNilValue {
				continue
			}
			ids[k], _ = strconv.ParseUint(value, 10, 64)
			k++
		}
		if hasValidValue {
			if k == 0 {
				if schema.hasLocalCache {
					schema.localCache.setList(orm, indexName, bindID, cacheNilValue)
				}
				return &emptyResultsIterator[E]{}
			}
			ids = ids[0:k]
			slices.Sort(ids)
			values := GetByIDs[E](orm, ids...)
			if schema.hasLocalCache {
				if values.Len() == 0 {
					schema.localCache.setList(orm, indexName, bindID, cacheNilValue)
				} else {
					if schema.hasLocalCache {
						schema.localCache.setList(orm, indexName, bindID, values.All())
					} else {
						schema.localCache.setList(orm, indexName, bindID, ids)
					}
				}
			}
			return values
		}
	}
	if schema.hasLocalCache {
		ids := SearchIDs[E](orm, index.CreteWhere(hasNil, attributes), nil)
		if len(ids) == 0 {
			schema.localCache.setList(orm, indexName, bindID, cacheNilValue)
			rc.SAdd(orm, redisSetKey, cacheNilValue)
			return &emptyResultsIterator[E]{}
		}
		idsForRedis := make([]any, len(ids))
		for i, value := range ids {
			idsForRedis[i] = strconv.FormatUint(value, 10)
		}
		p := orm.RedisPipeLine(rc.GetCode())
		p.Del(redisSetKey)
		p.SAdd(redisSetKey, redisValidSetValue)
		p.SAdd(redisSetKey, idsForRedis...)
		p.Exec(orm)
		values := GetByIDs[E](orm, ids...)
		if schema.hasLocalCache {
			schema.localCache.setList(orm, indexName, bindID, values.All())
		} else {
			schema.localCache.setList(orm, indexName, bindID, ids)
		}
		return values
	}
	values := Search[E](orm, index.CreteWhere(hasNil, attributes), nil)
	if values.Len() == 0 {
		rc.SAdd(orm, redisSetKey, redisValidSetValue, cacheNilValue)
	} else {
		idsForRedis := make([]any, values.Len()+1)
		idsForRedis[0] = redisValidSetValue
		i := 0
		for values.Next() {
			idsForRedis[i+1] = strconv.FormatUint(reflect.ValueOf(values.Entity()).Elem().Field(0).Uint(), 10)
			i++
		}
		values.Reset()
		rc.SAdd(orm, redisSetKey, idsForRedis...)
	}
	return values
}

func hashIndexAttributes(attributes []any) uint64 {
	attributesHash, err := jsoniter.ConfigFastest.Marshal(attributes)
	hash := fnv.New64()
	_, err = hash.Write(attributesHash)
	checkError(err)
	bindID := hash.Sum64()
	return bindID
}
