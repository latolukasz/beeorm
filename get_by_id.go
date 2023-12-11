package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByID[E any](c Context, id uint64) (entity *E) {
	var e E
	cE := c.(*contextImplementation)
	schema := cE.engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	value := getByID(cE, id, schema)
	if value == nil {
		return nil
	}
	return value.(*E)
}

func getByID(c *contextImplementation, id uint64, schema *entitySchema) (entity any) {
	if schema.hasLocalCache {
		e, has := schema.localCache.getEntity(c, id)
		if has {
			if e == nil {
				return
			}
			entity = e
			return
		}
	}
	cacheRedis, hasRedis := schema.GetRedisCache()
	var cacheKey string
	if hasRedis {
		cacheKey = schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
		row := cacheRedis.LRange(c, cacheKey, 0, int64(len(schema.columnNames)+1))
		l := len(row)
		if len(row) > 0 {
			if l == 1 {
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, id, nil)
				}
				return
			}
			value := reflect.New(schema.t)
			entity = value.Interface()
			if deserializeFromRedis(row, schema, value.Elem()) {
				if schema.hasLocalCache {
					schema.localCache.setEntity(c, id, entity)
				}
				return
			}
		}
	}
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE ID = ? LIMIT 1"
	pointers := prepareScan(schema)
	found := schema.GetDB().QueryRow(c, NewWhere(query, id), pointers...)
	if found {
		value := reflect.New(schema.t)
		entity = value.Interface()
		deserializeFromDB(schema.fields, value.Elem(), pointers)
	}
	if entity == nil {
		if schema.hasLocalCache {
			schema.localCache.setEntity(c, id, nil)
		}
		if hasRedis {
			p := c.RedisPipeLine(cacheRedis.GetCode())
			p.Del(cacheKey)
			p.RPush(cacheKey, cacheNilValue)
			p.Exec(c)
		}
		return
	}
	if schema.hasLocalCache {
		schema.localCache.setEntity(c, id, entity)
	}
	if hasRedis {
		bind := make(Bind)
		err := fillBindFromOneSource(c, bind, reflect.ValueOf(entity).Elem(), schema.fields, "")
		checkError(err)
		values := convertBindToRedisValue(bind, schema)
		cacheRedis.RPush(c, cacheKey, values...)
	}
	return
}
