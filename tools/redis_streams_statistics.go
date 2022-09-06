package tools

import (
	"math"
	"strconv"
	"strings"
	"time"

	orm "github.com/latolukasz/beeorm"
)

type RedisStreamStatistics struct {
	Stream             string
	RedisPool          string
	Len                uint64
	OldestEventSeconds int
	Groups             []*RedisStreamGroupStatistics
}

type RedisStreamGroupStatistics struct {
	Group                 string
	Pending               uint64
	LastDeliveredID       string
	LastDeliveredDuration time.Duration
	LowerID               string
	LowerDuration         time.Duration
	Consumers             []*RedisStreamConsumerStatistics
}

type RedisStreamConsumerStatistics struct {
	Name    string
	Pending uint64
}

func GetRedisStreamsStatistics(engine orm.Engine) []*RedisStreamStatistics {
	now := time.Now()
	results := make([]*RedisStreamStatistics, 0)
	for redisPool, channels := range engine.GetRegistry().GetRedisStreams() {
		r := engine.GetRedis(redisPool)
		for stream := range channels {
			stat := &RedisStreamStatistics{Stream: stream, RedisPool: redisPool}
			results = append(results, stat)
			stat.Groups = make([]*RedisStreamGroupStatistics, 0)
			stat.Len = uint64(r.XLen(stream))
			minPending := -1
			for _, group := range r.XInfoGroups(stream) {
				groupStats := &RedisStreamGroupStatistics{Group: group.Name, Pending: uint64(group.Pending)}
				groupStats.LastDeliveredID = group.LastDeliveredID
				groupStats.LastDeliveredDuration, _ = idToSince(group.LastDeliveredID, now)
				groupStats.Consumers = make([]*RedisStreamConsumerStatistics, 0)

				pending := r.XPending(stream, group.Name)
				if pending.Count > 0 {
					groupStats.LowerID = pending.Lower
					lower, t := idToSince(pending.Lower, now)
					groupStats.LowerDuration = lower
					if lower != 0 {
						since := time.Since(t)
						if minPending == -1 || int(since.Seconds()) > minPending {
							stat.OldestEventSeconds = int(since.Seconds())
							minPending = int(since.Seconds())
						}
					}
					for name, pending := range pending.Consumers {
						consumer := &RedisStreamConsumerStatistics{Name: name, Pending: uint64(pending)}
						groupStats.Consumers = append(groupStats.Consumers, consumer)
					}
				}
				stat.Groups = append(stat.Groups, groupStats)
			}
		}
	}
	return results
}

func idToSince(id string, now time.Time) (time.Duration, time.Time) {
	if id == "" || id == "0-0" {
		return 0, time.Now()
	}
	unixInt, _ := strconv.ParseInt(strings.Split(id, "-")[0], 10, 64)
	unix := time.Unix(0, unixInt*1000000)
	return time.Duration(int64(math.Max(float64(now.Sub(unix).Nanoseconds()), 0))), unix
}
