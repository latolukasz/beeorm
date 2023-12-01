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
	elem := reflect.ValueOf(entity).Elem()
	id := elem.Field(0).Uint()

	var flushPipeline *RedisPipeLine
	uniqueIndexes := schema.GetUniqueIndexes()
	if len(uniqueIndexes) > 0 {
		cache := c.Engine().Redis(schema.getForcedRedisCode())
		for indexName, indexColumns := range uniqueIndexes {
			indexChanged := false
			for _, column := range indexColumns {
				if column == field {
					indexChanged = true
					break
				}
			}
			if !indexChanged {
				continue
			}
			hSetKey := schema.getCacheKey() + ":" + indexName
			newBind := make(Bind)
			oldBind := make(Bind)
			newBind[field] = bindValue
			for _, column := range indexColumns {
				setter, _ = schema.fieldBindSetters[column]
				val, _ := setter(elem.FieldByName(column).Interface())
				if column != field {
					newBind[column] = val
				}
				oldBind[column] = val
			}

			hField, hasKey := buildUniqueKeyHSetField(schema, indexColumns, newBind)
			if hasKey {
				previousID, inUse := cache.HGet(c, hSetKey, hField)
				if inUse {
					idAsUint, _ := strconv.ParseUint(previousID, 10, 64)
					return &DuplicatedKeyBindError{Index: indexName, ID: idAsUint, Columns: indexColumns}
				}
				flushPipeline = c.RedisPipeLine(cache.GetConfig().GetCode())
				flushPipeline.HSet(hSetKey, hField, strconv.FormatUint(id, 10))
			}
			hFieldOld, hasKey := buildUniqueKeyHSetField(schema, indexColumns, oldBind)
			if hasKey {
				flushPipeline = c.RedisPipeLine(cache.GetConfig().GetCode())
				flushPipeline.HDel(hSetKey, hFieldOld)
			}
		}
	}

	if execute {
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
	if flushPipeline != nil {
		flushPipeline.Exec(c)
	}
	return nil
}
