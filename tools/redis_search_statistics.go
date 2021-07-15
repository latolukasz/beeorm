package tools

import (
	"strings"

	orm "github.com/latolukasz/beeorm"
)

type RedisSearchStatistics struct {
	Index    *orm.RedisSearchIndex
	Versions []*RedisSearchStatisticsIndexVersion
}

type RedisSearchStatisticsIndexVersion struct {
	Info    *orm.RedisSearchIndexInfo
	Current bool
}

func GetRedisSearchStatistics(engine *orm.Engine) []*RedisSearchStatistics {
	result := make([]*RedisSearchStatistics, 0)
	indices := engine.GetRegistry().GetRedisSearchIndices()
	for pool, list := range indices {
		search := engine.GetRedisSearch(pool)
		indicesInRedis := search.ListIndices()
		for _, index := range list {
			stat := &RedisSearchStatistics{Index: index, Versions: make([]*RedisSearchStatisticsIndexVersion, 0)}
			current := ""
			info := search.Info(index.Name)
			if info != nil {
				current = info.Name
			}
			for _, inRedis := range indicesInRedis {
				if strings.HasPrefix(inRedis, index.Name+":") {
					info := search.Info(inRedis)
					indexStats := &RedisSearchStatisticsIndexVersion{Info: info, Current: current == inRedis}
					stat.Versions = append(stat.Versions, indexStats)
				}
			}
			result = append(result, stat)
		}
	}
	return result
}
