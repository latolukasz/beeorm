package tools

import (
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

type RedisStreamGroupSpeedStatistics struct {
	SpeedEvents                      int64
	SpeedMilliseconds                float64
	DBQueriesPerEvent                float64
	DBQueriesMillisecondsPerEvent    float64
	RedisQueriesPerEvent             float64
	RedisQueriesMillisecondsPerEvent float64
}

type RedisStreamGroupStatistics struct {
	Group                 string
	Pending               uint64
	LastDeliveredID       string
	LastDeliveredDuration time.Duration
	LowerID               string
	LowerDuration         time.Duration
	*RedisStreamGroupSpeedStatistics
	Consumers    []*RedisStreamConsumerStatistics
	SpeedHistory []*RedisStreamGroupSpeedStatistics
}

type RedisStreamConsumerStatistics struct {
	Name    string
	Pending uint64
}

func GetRedisStreamsStatistics(engine *orm.Engine) []*RedisStreamStatistics {
	now := time.Now()
	results := make([]*RedisStreamStatistics, 0)
	for redisPool, channels := range engine.GetRegistry().GetRedisStreams() {
		r := engine.GetRedis(redisPool)
		today := now.Format("01-02-06")
		for stream := range channels {
			stat := &RedisStreamStatistics{Stream: stream, RedisPool: redisPool}
			results = append(results, stat)
			stat.Groups = make([]*RedisStreamGroupStatistics, 0)
			stat.Len = uint64(r.XLen(stream))
			minPending := -1
			for _, group := range r.XInfoGroups(stream) {
				groupStats := &RedisStreamGroupStatistics{Group: group.Name, Pending: uint64(group.Pending)}
				groupStats.RedisStreamGroupSpeedStatistics = &RedisStreamGroupSpeedStatistics{}
				calculateSpeedStatistics(groupStats.RedisStreamGroupSpeedStatistics, group.Name, redisPool, r.HGetAll("_orm_ss"+today))
				groupStats.SpeedHistory = make([]*RedisStreamGroupSpeedStatistics, 7)
				date := now
				for i := 1; i <= 7; i++ {
					date = date.Add(time.Hour * -24)
					stats := &RedisStreamGroupSpeedStatistics{}
					calculateSpeedStatistics(stats, group.Name, redisPool, r.HGetAll("_orm_ss"+date.Format("01-02-06")))
					groupStats.SpeedHistory[i-1] = stats
				}
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

func calculateSpeedStatistics(groupStats *RedisStreamGroupSpeedStatistics, group, redisPool string, speedStats map[string]string) {
	speedKey := group + "_" + redisPool
	events, has := speedStats[speedKey+"e"]
	if has {
		speedEventsAsInt, _ := strconv.Atoi(events)
		if speedEventsAsInt > 0 {
			speedEvents := int64(speedEventsAsInt)
			speedTime, _ := strconv.Atoi(speedStats[speedKey+"t"])
			groupStats.SpeedMilliseconds = float64(speedTime) / 1000 / float64(speedEvents)
			groupStats.SpeedEvents = speedEvents

			dbQueries, _ := strconv.Atoi(speedStats[speedKey+"d"])
			if dbQueries > 0 {
				groupStats.DBQueriesPerEvent = float64(dbQueries) / float64(speedEvents)
				dbTime, _ := strconv.Atoi(speedStats[speedKey+"dt"])
				groupStats.DBQueriesMillisecondsPerEvent = float64(dbTime) / 1000 / float64(speedEvents)
			}
			redisQueries, _ := strconv.Atoi(speedStats[speedKey+"r"])
			if redisQueries > 0 {
				groupStats.RedisQueriesPerEvent = float64(redisQueries) / float64(speedEvents)
				redisTime, _ := strconv.Atoi(speedStats[speedKey+"rt"])
				groupStats.RedisQueriesMillisecondsPerEvent = float64(redisTime) / 1000 / float64(speedEvents)
			}
		}
	}
}

func idToSince(id string, now time.Time) (time.Duration, time.Time) {
	if id == "" || id == "0-0" {
		return 0, time.Now()
	}
	unixInt, _ := strconv.ParseInt(strings.Split(id, "-")[0], 10, 64)
	unix := time.Unix(0, unixInt*1000000)
	s := now.Sub(unix)
	if s < 0 {
		return 0, time.Now()
	}
	return s, unix
}
