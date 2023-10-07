package beeorm

import (
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPipeLine struct {
	c        Context
	r        *redisCache
	pool     string
	pipeLine redis.Pipeliner
	commands int
	log      []string
}

func (rp *RedisPipeLine) LPush(key string, values ...interface{}) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LPUSH %s %v", key, values))
	}
	rp.pipeLine.LPush(rp.c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) RPush(key string, values ...interface{}) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("RPUSH %s %v", key, values))
	}
	rp.pipeLine.RPush(rp.c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) LSet(key string, index int64, value interface{}) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LSET %s %d %v", key, index, value))
	}
	rp.pipeLine.LSet(rp.c.Ctx(), key, index, value)
}

func (rp *RedisPipeLine) Del(key ...string) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "DEL "+strings.Join(key, " "))
	}
	rp.pipeLine.Del(rp.c.Ctx(), key...)
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "GET "+key)
	}
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(rp.c.Ctx(), key)}
}

func (rp *RedisPipeLine) LRange(key string, start, stop int64) *PipeLineSlice {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LRANGE %s %d %d", key, start, stop))
	}
	return &PipeLineSlice{p: rp, cmd: rp.pipeLine.LRange(rp.c.Ctx(), key, start, stop)}
}

func (rp *RedisPipeLine) Set(key string, value interface{}, expiration time.Duration) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("SET %s %v %s", key, value, expiration.String()))
	}
	rp.pipeLine.Set(rp.c.Ctx(), key, value, expiration)
}

func (rp *RedisPipeLine) SAdd(key string, members ...interface{}) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("SADD %s %v", key, members))
	}
	rp.pipeLine.SAdd(rp.c.Ctx(), key, members...)
}

func (rp *RedisPipeLine) MSet(pairs ...interface{}) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		rp.log = append(rp.log, "MSET "+message)
	}
	rp.pipeLine.MSet(rp.c.Ctx(), pairs...)
}

func (rp *RedisPipeLine) Expire(key string, expiration time.Duration) *PipeLineBool {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("EXPIRE %s %s", key, expiration.String()))
	}
	return &PipeLineBool{p: rp, cmd: rp.pipeLine.Expire(rp.c.Ctx(), key, expiration)}
}

func (rp *RedisPipeLine) HIncrBy(key, field string, incr int64) *PipeLineInt {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("HINCRBY %s %s %d", key, field, incr))
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HIncrBy(rp.c.Ctx(), key, field, incr)}
}

func (rp *RedisPipeLine) HSet(key string, values ...interface{}) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("HSET %s %v", key, values))
	}
	rp.pipeLine.HSet(rp.c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) HDel(key string, values ...string) {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("HDEL %s %s", key, strings.Join(values, " ")))
	}
	rp.pipeLine.HDel(rp.c.Ctx(), key, values...)
}

func (rp *RedisPipeLine) XAdd(stream string, values []string) *PipeLineString {
	rp.commands++
	hasLog, _ := rp.c.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("XADD %s %s", stream, strings.Join(values, " ")))
	}
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(rp.c.Ctx(), &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec(c Context) {
	if rp.commands == 0 {
		return
	}
	hasLog, loggers := rp.c.getRedisLoggers()
	start := getNow(hasLog)
	_, err := rp.pipeLine.Exec(rp.c.Ctx())
	rp.pipeLine = rp.r.client.Pipeline()
	if err != nil && err == redis.Nil {
		err = nil
	}
	if hasLog {
		query := strings.Join(rp.log, "\n\u001B[38;2;255;255;155m")
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

type PipeLineSlice struct {
	p   *RedisPipeLine
	cmd *redis.StringSliceCmd
}

func (c *PipeLineSlice) Result() []string {
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
