package beeorm

import (
	"fmt"
	"reflect"
	"strings"
)

func GetByIDs(c Context, ids []uint64, entities interface{}, references ...string) (found bool) {
	_, hasMissing := getByIDs(c, ids, reflect.ValueOf(entities), references)
	return !hasMissing
}

func getByIDs(c Context, ids []uint64, entities reflect.Value, references []string) (schema EntitySchema, hasMissing bool) {
	schema = c.Engine().Registry().getEntitySchemaForSlice(entities.Type())
	resultsSlice := entities.Elem()
	diffCap := len(ids) - resultsSlice.Cap()
	if diffCap > 0 {
		resultsSlice.Grow(diffCap)
	}
	resultsSlice.SetLen(len(ids))
	if len(ids) == 0 {
		return
	}

	//TODO slooooow!!
	for i := range ids {
		resultsSlice.Index(i).SetZero()
	}

	cacheLocal, hasLocalCache := schema.GetLocalCache()
	cacheRedis, hasRedisCache := schema.GetRedisCache()

	foundInCache := 0
	hasCacheNils := false
	if hasLocalCache {
		for i, id := range ids {
			fromLocalCache, hasInLocalCache := cacheLocal.getEntity(c, id)
			if hasInLocalCache {
				if fromLocalCache == emptyReflect {
					resultsSlice.Index(i).Set(fromLocalCache)
					hasMissing = true
					hasCacheNils = true
				}
				foundInCache++
			}
		}
	}
	if hasRedisCache && foundInCache < len(ids) {
		redisHSetKeys := getMissingIdsFromResults(ids, foundInCache, resultsSlice)
		fromRedisAll := cacheRedis.hMGetUints(c, schema.GetCacheKey(), redisHSetKeys...)
		if foundInCache == 0 {
			for i := range redisHSetKeys {
				fromRedisCache := fromRedisAll[i]
				if fromRedisCache != nil {
					entity := schema.NewEntity()
					resultsSlice.Index(i).Set(entity.getORM().value)
					if fromRedisCache != cacheNilValue {
						fillFromBinary(c, schema, []byte(fromRedisCache.(string)), entity)
					} else {
						hasMissing = true
						hasCacheNils = true
					}
					foundInCache++
				}
			}
		} else {
			for k, id := range redisHSetKeys {
				fromRedisCache := fromRedisAll[k]
				if fromRedisCache != nil {
					for i, idOriginal := range ids {
						if id == idOriginal {
							entity := schema.NewEntity()
							resultsSlice.Index(i).Set(entity.getORM().value)
							if fromRedisCache != cacheNilValue {
								fillFromBinary(c, schema, []byte(fromRedisCache.(string)), entity)
								if hasLocalCache {
									cacheLocal.setEntity(c, id, entity.getORM().value)
								}
							} else {
								hasMissing = true
								hasCacheNils = true
								if hasLocalCache {
									cacheLocal.setEntity(c, id, emptyReflect)
								}
							}
							foundInCache++
						}
					}
				}
			}
		}
	}
	if foundInCache < len(ids) {
		var redisHSetValues []interface{}
		dbIDs := getMissingIdsFromResults(ids, foundInCache, resultsSlice)
		idsQuery := strings.ReplaceAll(fmt.Sprintf("%v", dbIDs), " ", ",")[1:]
		query := "SELECT " + schema.getFieldsQuery() + " FROM `" + schema.GetTableName() + "` WHERE `ID` IN (" + idsQuery[:len(idsQuery)-1] + ")"
		results, def := schema.GetDB().Query(c, query)
		defer def()
		foundInDB := 0
		for results.Next() {
			foundInDB++
			pointers := prepareScan(schema)
			results.Scan(pointers...)
			entity := schema.NewEntity()
			fillFromDBRow(c, schema, pointers, entity)
			id := *pointers[0].(*uint64)
			for i, originalID := range ids {
				if id == originalID {
					resultsSlice.Index(i).Set(entity.getORM().value)
				}
			}
			if hasLocalCache {
				cacheLocal.setEntity(c, id, entity.getORM().value)
			}
			if hasRedisCache {
				if len(ids) == 1 {
					cacheRedis.HSet(c, schema.GetCacheKey(), id, string(entity.getORM().binary))
				} else {
					redisHSetValues = append(redisHSetValues, id, string(entity.getORM().binary))
				}
			}
		}
		def()
		if redisHSetValues != nil {
			cacheRedis.HSet(c, schema.GetCacheKey(), redisHSetValues...)
		}
		if foundInDB < len(dbIDs) {
			for i, id := range ids {
				if resultsSlice.Index(i).IsZero() {
					hasMissing = true
					if !hasLocalCache && !hasRedisCache {
						break
					}
					if hasLocalCache {
						cacheLocal.setEntity(c, id, emptyReflect)
					}
					if hasRedisCache {
						cacheRedis.HSet(c, schema.GetCacheKey(), id, cacheNilValue)
					}
				}
			}
		}
	}
	if hasCacheNils {
		for i := range ids {
			inSlice := resultsSlice.Index(i)
			if inSlice.Interface().(Entity).GetID() == 0 {
				inSlice.SetZero()
			}
		}
	}
	return
}

func getMissingIdsFromResults(ids []uint64, foundInCache int, slice reflect.Value) []uint64 {
	if foundInCache == 0 {
		return ids
	}
	result := make([]uint64, len(ids)-foundInCache)
	j := 0
	for i, id := range ids {
		if slice.Index(i).IsZero() {
			result[j] = id
			j++
		}
	}
	return result
}
