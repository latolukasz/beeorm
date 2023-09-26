package beeorm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPipeLine struct {
	r        *redisCache
	pool     string
	pipeLine redis.Pipeliner
	commands int
	log      []string
}

func (rp *RedisPipeLine) LPush(c Context, key string, values ...interface{}) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "LPUSH")
	}
	rp.pipeLine.LPush(c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) RPush(c Context, key string, values ...interface{}) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "RPUSH")
	}
	rp.pipeLine.RPush(c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) LSet(c Context, key string, index int64, value interface{}) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LSET %s %d %v", key, index, value))
	}
	rp.pipeLine.LSet(c.Ctx(), key, index, value)
}

func (rp *RedisPipeLine) Del(c Context, key ...string) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "DEL")
		rp.log = append(rp.log, key...)
	}
	rp.pipeLine.Del(c.Ctx(), key...)
}

func (rp *RedisPipeLine) Get(c Context, key string) *PipeLineGet {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "GET", key)
	}
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(c.Ctx(), key)}
}

func (rp *RedisPipeLine) Set(c Context, key string, value interface{}, expiration time.Duration) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "SET", key, expiration.String())
	}
	rp.pipeLine.Set(c.Ctx(), key, value, expiration)
}

func (rp *RedisPipeLine) MSet(c Context, pairs ...interface{}) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		rp.log = append(rp.log, "MSET", message)
	}
	rp.pipeLine.MSet(c.Ctx(), pairs...)
}

func (rp *RedisPipeLine) Expire(c Context, key string, expiration time.Duration) *PipeLineBool {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "EXPIRE", key, expiration.String())
	}
	return &PipeLineBool{p: rp, cmd: rp.pipeLine.Expire(c.Ctx(), key, expiration)}
}

func (rp *RedisPipeLine) HIncrBy(c Context, key, field string, incr int64) *PipeLineInt {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "HINCRBY", key, field, strconv.Itoa(int(incr)))
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HIncrBy(c.Ctx(), key, field, incr)}
}

func (rp *RedisPipeLine) HSet(c Context, key string, values ...interface{}) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "HSET", key)
		for _, v := range values {
			rp.log = append(rp.log, fmt.Sprintf("%v", v))
		}
	}
	rp.pipeLine.HSet(c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) HDel(c Context, key string, values ...string) {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "HDEL", key)
		rp.log = append(rp.log, values...)
	}
	rp.pipeLine.HDel(c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) XAdd(c Context, stream string, values []string) *PipeLineString {
	rp.commands++
	hasLog, _ := c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "XADD", stream)
		rp.log = append(rp.log, values...)
	}
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(c.Ctx(), &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec(c Context) {
	if rp.commands == 0 {
		return
	}
	hasLog, loggers := c.getRedisLoggers()
	start := getNow(hasLog)
	_, err := rp.pipeLine.Exec(c.Ctx())
	rp.pipeLine = rp.r.client.Pipeline()
	if err != nil && err == redis.Nil {
		err = nil
	}
	if hasLog {
		query := strings.Join(rp.log, " ")
		fillLogFields(c, loggers, rp.pool, sourceRedis, "PIPELINE EXEC", query, start, false, err)
	}
	rp.log = nil
	rp.commands = 0
	checkError(err)
}

type PipeLineGet struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineGet) Result() (value string, has bool) {
	val, err := c.cmd.Result()
	if err == redis.Nil {
		return val, false
	}
	checkError(err)
	return val, true
}

type PipeLineString struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineString) Result() string {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}

type PipeLineInt struct {
	p   *RedisPipeLine
	cmd *redis.IntCmd
}

func (c *PipeLineInt) Result() int64 {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}

type PipeLineBool struct {
	p   *RedisPipeLine
	cmd *redis.BoolCmd
}

func (c *PipeLineBool) Result() bool {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}
