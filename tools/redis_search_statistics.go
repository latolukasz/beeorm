package tools

import (
	orm "github.com/latolukasz/beeorm"
)

type RedisSearchStatistics struct {
	Index *orm.RedisSearchIndex
	Info  *orm.RedisSearchIndexInfo
}

func GetRedisSearchStatistics(engine *orm.Engine) []*RedisSearchStatistics {
	result := make([]*RedisSearchStatistics, 0)
	indices := engine.GetRegistry().GetRedisSearchIndices()
	for pool, list := range indices {
		search := engine.GetRedisSearch(pool)
		for _, index := range list {
			info := search.Info(index.Name)
			stat := &RedisSearchStatistics{Index: index, Info: info}
			result = append(result, stat)
		}
	}
	return result
}
