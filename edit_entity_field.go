package beeorm

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func EditEntityField[E any](c Context, entity *E, field string, value any, execute bool) error {
	schema := getEntitySchema[E](c)
	setter, has := schema.fieldBindSetters[field]
	if !has {
		return fmt.Errorf("field '%s' not found", field)
	}
	newValue, err := setter(value)
	if err != nil {
		return err
	}
	elem := reflect.ValueOf(entity).Elem()
	getter := schema.fieldGetters[field]
	oldValue, err := setter(getter(elem))
	if err != nil {
		panic(err)
	}
	if oldValue == newValue {
		return nil
	}
	id := elem.Field(0).Uint()

	var flushPipeline *RedisPipeLine
	uniqueIndexes := schema.GetUniqueIndexes()
	var newBind Bind
	var oldBind Bind
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
			newBind = make(Bind)
			oldBind = make(Bind)
			newBind[field] = newValue
			oldBind[field] = oldValue
			if len(indexColumns) > 1 {
				for _, column := range indexColumns {
					if column != field {
						setter, _ = schema.fieldBindSetters[column]
						val, _ := setter(elem.FieldByName(column).Interface())
						newBind[column] = val
						oldBind[column] = val
					}
				}
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
		schema.GetDB().Exec(c, sql, newValue, id)
		fSetter := schema.fieldSetters[field]
		if schema.hasLocalCache {
			func() {
				schema.localCache.mutex.Lock()
				defer schema.localCache.mutex.Unlock()
				fSetter(newValue, elem)
			}()
		} else {
			fSetter(newValue, elem)
		}
		if schema.hasRedisCache {
			index := int64(schema.columnMapping[field] + 1)
			rKey := schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
			schema.redisCache.LSet(c, rKey, index, convertBindValueToRedisValue(newValue))
		}
	}

	for columnName := range schema.cachedReferences {
		if columnName != field {
			continue
		}
		refColumn := columnName

		newAsInt := uint64(0)
		oldAsInt := uint64(0)
		if newValue != nil {
			newAsInt, _ = newValue.(uint64)
		}
		if oldValue != nil {
			oldAsInt, _ = oldValue.(uint64)
		}
		if oldAsInt > 0 {
			if schema.hasLocalCache {
				schema.localCache.removeReference(c, refColumn, oldAsInt)
			}
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(oldAsInt, 10)
			flushPipeline = c.RedisPipeLine(schema.getForcedRedisCode())
			flushPipeline.SRem(redisSetKey, strconv.FormatUint(id, 10))
		}
		if newAsInt > 0 {
			if schema.hasLocalCache {
				schema.localCache.removeReference(c, refColumn, newAsInt)
			}
			redisSetKey := schema.cacheKey + ":" + refColumn + ":" + strconv.FormatUint(newAsInt, 10)
			flushPipeline = c.RedisPipeLine(schema.getForcedRedisCode())
			flushPipeline.SAdd(redisSetKey, strconv.FormatUint(id, 10))
		}
	}

	logTableSchema, hasLogTable := c.Engine().Registry().(*engineRegistryImplementation).entityLogSchemas[schema.t]
	if hasLogTable {
		data := make([]any, 7)
		data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`,`After`) VALUES(?,?,?,?,?,?)"
		data[1] = strconv.FormatUint(logTableSchema.uuid(), 10)
		data[2] = strconv.FormatUint(id, 10)
		data[3] = time.Now().Format(time.DateTime)
		if len(c.GetMetaData()) > 0 {
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(c.GetMetaData())
			data[4] = asJSON
		} else {
			data[4] = nil
		}
		if oldBind == nil {
			oldBind = Bind{field: oldValue}
		}
		if newBind == nil {
			newBind = Bind{field: newValue}
		}
		asJSON, _ := jsoniter.ConfigFastest.MarshalToString(oldBind)
		data[5] = asJSON
		asJSON, _ = jsoniter.ConfigFastest.MarshalToString(newBind)
		data[6] = asJSON
		asJSON, _ = jsoniter.ConfigFastest.MarshalToString(data)
		pipeline := c.RedisPipeLine(schema.getForcedRedisCode())
		pipeline.RPush(logTableSchema.asyncCacheKey, asJSON)
		if flushPipeline == nil || flushPipeline.r.config.GetCode() != pipeline.r.config.GetCode() {
			pipeline.Exec(c)
		}
	}

	if flushPipeline != nil {
		flushPipeline.Exec(c)
	}
	return nil
}
