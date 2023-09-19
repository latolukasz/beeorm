package beeorm

import (
	"fmt"
	"strconv"
	"strings"
)

const loadingUniqueKeysPage = 5000

func LoadUniqueKeys(c Context, debug bool) {
	for _, entity := range c.Engine().Registry().Entities() {
		schema := c.Engine().Registry().EntitySchema(entity)
		indexes := schema.GetUniqueIndexes()
		if len(indexes) == 0 {
			continue
		}
		cache, hasRedis := schema.GetRedisCache()
		if !hasRedis {
			cache = c.Engine().Redis(DefaultPoolCode)
		}
		db := schema.GetDB()
		for indexName, columns := range schema.GetUniqueIndexes() {
			hSetKey := schema.GetCacheKey() + ":" + indexName
			if len(columns) == 0 || cache.Exists(c, hSetKey) > 0 {
				continue
			}
			where := NewWhere("")
			pointers := make([]interface{}, len(columns)+1)
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
			whereCount := NewWhere("SELECT COUNT(`ID`) FROM `" + schema.GetTableName() + "` WHERE " + where.String())
			selectWhere.Append(" FROM `" + schema.GetTableName() + "` WHERE ID > ? AND")
			selectWhere.Append(where.String())

			if debug {
				poolTemplate := "\u001B[1m\x1b[38;2;175;175;175;48;2;255;255;255m%-94s\u001B[0m\x1b[0m\u001B[0m\n"
				row := beeORMLogo
				title := fmt.Sprintf("Loading unique key '%s' from %s into redis", indexName, schema.GetType().String())
				row += fmt.Sprintf(poolTemplate, title)
				print(row)
				print(".")
			}
			total := uint64(0)
			db.QueryRow(c, whereCount, &total)
			if total == 0 {
				if debug {
					print(strings.Repeat(".", 100))
					println("[DONE]")
				}
				continue
			}
			func() {
				p, cl := db.Prepare(c, selectWhere.String())
				defer cl()
				lastID := uint64(0)
				dotsPrinted := 0
				executed := uint64(0)
				for {
					count := 0
					func() {
						rows, cl2 := p.Query(c, lastID)
						defer cl2()
						for rows.Next() {
							rows.Scan(pointers...)
							id := *pointers[0].(*string)
							lastID, _ = strconv.ParseUint(id, 10, 64)
							hField := ""
							for i := 1; i < len(pointers); i++ {
								hField += *pointers[i].(*string)
							}
							cache.HSet(c, hSetKey, hField, id)
							count++
							executed++
						}
						cl2()
					}()
					if debug {
						dotsToPrint := int((float64(executed) / float64(total)) * 100)
						diff := dotsToPrint - dotsPrinted
						if diff > 0 {
							fmt.Print(strings.Repeat(".", diff))
						}
					}
					if debug {
						fmt.Print("\n")
					}
					if count < loadingUniqueKeysPage {
						break
					}
				}
				cl()
			}()
		}
	}
}
