package tools

import (
	"sort"
	"strings"

	orm "github.com/latolukasz/beeorm"
)

type RedisStatistics struct {
	RedisPool string
	Info      map[string]string
}

func GetRedisStatistics(engine orm.Engine) []*RedisStatistics {
	pools := getRedisPools(engine)
	results := make([]*RedisStatistics, len(pools))
	for i, pool := range pools {
		poolStats := &RedisStatistics{RedisPool: pool, Info: make(map[string]string)}
		r := engine.GetRedis(pool)
		info := r.Info("everything")
		lines := strings.Split(info, "\r\n")
		for _, line := range lines {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			row := strings.Split(line, ":")
			val := ""
			if len(row) > 1 {
				val = row[1]
			}
			poolStats.Info[row[0]] = val
		}
		results[i] = poolStats
	}
	return results
}

func getRedisPools(engine orm.Engine) []string {
	pools := make([]string, 0)
	groupedByAddress := make(map[string][]string)
	for code, v := range engine.GetRegistry().GetRedisPools() {
		key := v.GetAddress()
		groupedByAddress[key] = append(groupedByAddress[key], code)
	}
	for _, codes := range groupedByAddress {
		sort.Strings(codes)
		pools = append(pools, codes[0])
	}
	return pools
}
