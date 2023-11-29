package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func EditEntityField[E any](c Context, entity *E, field string, value any, execute bool) error {
	schema := getEntitySchema[E](c)
	setter, has := schema.fieldBindSetters[field]
	if !has {
		return fmt.Errorf("field '%s' not found", field)
	}
	bindValue, err := setter(value)
	if err != nil {
		return err
	}
	if execute {
		elem := reflect.ValueOf(entity).Elem()
		id := elem.Field(0).Uint()
		sql := "UPDATE `" + schema.GetTableName() + "` SET `" + field + "` = ? WHERE ID = ?"
		schema.GetDB().Exec(c, sql, bindValue, id)
		fSetter := schema.fieldSetters[field]
		if schema.hasLocalCache {
			func() {
				schema.localCache.mutex.Lock()
				defer schema.localCache.mutex.Unlock()
				fSetter(bindValue, elem)
			}()
		} else {
			fSetter(bindValue, elem)
		}
		if schema.hasRedisCache {
			index := int64(schema.columnMapping[field] + 1)
			rKey := schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
			schema.redisCache.LSet(c, rKey, index, convertBindValueToRedisValue(bindValue))
		}
	}
	return nil
}
