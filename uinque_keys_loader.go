package beeorm

import (
	"strconv"
	"time"
)

const loadingUniqueKeysPage = 5000

func LoadUniqueKeys(orm ORM, force bool, entities ...EntitySchema) (inserted int) {
	lock, got := orm.Engine().Redis(DefaultPoolCode).GetLocker().Obtain(orm, "load_unique_key", time.Second*5, 0)
	if !got {
		return
	}
	defer lock.Release(orm)
	schemas := entities
	if len(entities) == 0 {
		schemas = orm.Engine().Registry().Entities()
	}
	for _, schema := range schemas {
		indexes := schema.GetUniqueIndexes()
		if len(indexes) == 0 {
			continue
		}
		cache, hasRedis := schema.GetRedisCache()
		if !hasRedis {
			cache = orm.Engine().Redis(DefaultPoolCode)
		}
		db := schema.GetDB()
		for indexName, columns := range schema.GetUniqueIndexes() {
			if len(columns) == 0 {
				continue
			}
			hSetKey := schema.getCacheKey() + ":" + indexName
			if force {
				cache.Del(orm, hSetKey)
			} else if cache.Exists(orm, hSetKey) > 0 {
				_, isValid := cache.HGet(orm, hSetKey, "_is_valid")
				if isValid {
					continue
				}
			}

			where := NewWhere("")
			pointers := make([]any, len(columns)+1)
			var v string
			pointers[0] = &v
			selectWhere := NewWhere("SELECT `ID`")
			for i, column := range columns {
				if i > 0 {
					where.Append(" AND ")
				}
				where.Append(" `" + column + "` IS NOT NULL")
				selectWhere.Append(",`" + column + "`")
				var val string
				pointers[i+1] = &val
			}
			where.Append(" ORDER BY `ID`")
			whereCount := "SELECT COUNT(`ID`) FROM `" + schema.GetTableName() + "` WHERE " + where.String()
			selectWhere.Append(" FROM `" + schema.GetTableName() + "` WHERE ID > ? AND")
			selectWhere.Append(where.String())

			total := uint64(0)
			db.QueryRow(orm, NewWhere(whereCount), &total)
			if total == 0 {
				cache.HSet(orm, hSetKey, "", 0)
				continue
			}
			func() {
				p, cl := db.Prepare(orm, selectWhere.String())
				defer cl()
				lastID := uint64(0)
				executed := uint64(0)
				for {
					count := 0
					func() {
						rows, cl2 := p.Query(orm, lastID)
						defer cl2()
						for rows.Next() {
							rows.Scan(pointers...)
							id := *pointers[0].(*string)
							lastID, _ = strconv.ParseUint(id, 10, 64)
							hField := ""
							for i := 1; i < len(pointers); i++ {
								hField += *pointers[i].(*string)
							}
							cache.HSet(orm, hSetKey, hashString(hField), id)
							count++
							executed++
							inserted++
						}
						cl2()
					}()
					if count < loadingUniqueKeysPage {
						break
					}
				}
				cl()
			}()
			cache.HSet(orm, hSetKey, "_is_valid", "1")
		}
	}
	return inserted
}
