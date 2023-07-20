package beeorm

import (
	"math"
	"strconv"
	"strings"
	"time"
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
	Lag                   int64
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

func (eb *eventBroker) GetStreamStatistics(c Context, stream string) *RedisStreamStatistics {
	stats := eb.GetStreamsStatistics(c, stream)
	if len(stats) > 0 {
		return stats[0]
	}
	return nil
}

func (eb *eventBroker) GetStreamGroupStatistics(c Context, stream, group string) *RedisStreamGroupStatistics {
	stats := eb.GetStreamStatistics(c, stream)
	for _, groupStats := range stats.Groups {
		if groupStats.Group == group {
			return groupStats
		}
	}
	return &RedisStreamGroupStatistics{
		Group: group,
		Lag:   int64(stats.Len),
	}
}

func (eb *eventBroker) GetStreamsStatistics(c Context, stream ...string) []*RedisStreamStatistics {
	now := time.Now()
	results := make([]*RedisStreamStatistics, 0)
	for redisPool, channels := range eb.c.Engine().GetRedisStreams() {
		r := eb.c.Engine().GetRedis(redisPool)
		for streamName := range channels {
			validName := len(stream) == 0
			if !validName {
				for _, name := range stream {
					if name == streamName {
						validName = true
						break
					}
				}
			}
			if !validName {
				continue
			}
			stat := &RedisStreamStatistics{Stream: streamName, RedisPool: redisPool}
			results = append(results, stat)
			stat.Groups = make([]*RedisStreamGroupStatistics, 0)
			stat.Len = uint64(r.XLen(c, streamName))
			minPending := -1
			for _, group := range r.XInfoGroups(c, streamName) {
				groupStats := &RedisStreamGroupStatistics{Group: group.Name, Pending: uint64(group.Pending)}
				groupStats.LastDeliveredID = group.LastDeliveredID
				groupStats.Lag = group.Lag
				groupStats.LastDeliveredDuration, _ = idToSince(group.LastDeliveredID, now)
				groupStats.Consumers = make([]*RedisStreamConsumerStatistics, 0)

				pending := r.XPending(c, streamName, group.Name)
				groupStats.LowerID = pending.Lower
				if pending.Count > 0 {
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
