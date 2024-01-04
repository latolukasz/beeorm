package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByID[E any](orm ORM, id uint64) (entity *E) {
	var e E
	cE := orm.(*ormImplementation)
	schema := cE.engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	value, _ := getByID(cE, id, schema)
	if value == nil {
		return nil
	}
	return value.(*E)
}

func getByID(orm *ormImplementation, id uint64, schema *entitySchema) (entity any, cacheHit bool) {
	cacheHit = true
	if schema.hasLocalCache {
		e, has := schema.localCache.getEntity(orm, id)
		if has {
			if e == nil {
				return
			}
			entity = e
			return
		}
		cacheHit = false
	}
	cacheRedis, hasRedis := schema.GetRedisCache()
	var cacheKey string
	if hasRedis {
		cacheKey = schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
		row := cacheRedis.LRange(orm, cacheKey, 0, int64(len(schema.columnNames)+1))
		l := len(row)
		if len(row) > 0 {
			if l == 1 {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, nil)
				}
				return
			}
			value := reflect.New(schema.t)
			entity = value.Interface()
			if deserializeFromRedis(row, schema, value.Elem()) {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, entity)
				}
				return
			}
		}
	}
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE ID = ? LIMIT 1"
	pointers := prepareScan(schema)
	found := schema.GetDB().QueryRow(orm, NewWhere(query, id), pointers...)
	if found {
		value := reflect.New(schema.t)
		entity = value.Interface()
		deserializeFromDB(schema.fields, value.Elem(), pointers)
	}
	if entity == nil {
		if schema.hasLocalCache {
			schema.localCache.setEntity(orm, id, nil)
		}
		if hasRedis {
			p := orm.RedisPipeLine(cacheRedis.GetCode())
			p.Del(cacheKey)
			p.RPush(cacheKey, cacheNilValue)
			p.Exec(orm)
		}
		return
	}
	if schema.hasLocalCache {
		schema.localCache.setEntity(orm, id, entity)
	}
	if hasRedis {
		bind := make(Bind)
		err := fillBindFromOneSource(orm, bind, reflect.ValueOf(entity).Elem(), schema.fields, "")
		checkError(err)
		values := convertBindToRedisValue(bind, schema)
		cacheRedis.RPush(orm, cacheKey, values...)
	}
	return
}
