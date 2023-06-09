package beeorm

import (
	"context"
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

func (rp *RedisPipeLine) Del(key ...string) {
	for i, v := range key {
		key[i] = rp.r.addNamespacePrefix(v)
	}
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "DEL")
		rp.log = append(rp.log, key...)
	}
	rp.pipeLine.Del(context.Background(), key...)
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	key = rp.r.addNamespacePrefix(key)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "GET", key)
	}
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(context.Background(), key)}
}

func (rp *RedisPipeLine) Set(key string, value interface{}, expiration time.Duration) {
	key = rp.r.addNamespacePrefix(key)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "SET", key, expiration.String())
	}
	rp.pipeLine.Set(context.Background(), key, value, expiration)
}

func (rp *RedisPipeLine) MSet(pairs ...interface{}) {
	if rp.r.config.HasNamespace() {
		for i := 0; i < len(pairs); i = i + 2 {
			pairs[i] = rp.r.addNamespacePrefix(pairs[i].(string))
		}
	}
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		rp.log = append(rp.log, "MSET", message)
	}
	rp.pipeLine.MSet(context.Background(), pairs...)
}

func (rp *RedisPipeLine) Expire(key string, expiration time.Duration) *PipeLineBool {
	key = rp.r.addNamespacePrefix(key)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "EXPIRE", key, expiration.String())
	}
	return &PipeLineBool{p: rp, cmd: rp.pipeLine.Expire(context.Background(), key, expiration)}
}

func (rp *RedisPipeLine) HIncrBy(key, field string, incr int64) *PipeLineInt {
	key = rp.r.addNamespacePrefix(key)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "HINCRBY", key, field, strconv.Itoa(int(incr)))
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HIncrBy(context.Background(), key, field, incr)}
}

func (rp *RedisPipeLine) HSet(key string, values ...interface{}) {
	key = rp.r.addNamespacePrefix(key)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "HSET", key)
		for _, v := range values {
			rp.log = append(rp.log, fmt.Sprintf("%v", v))
		}
	}
	rp.pipeLine.HSet(context.Background(), key, values...)
}

func (rp *RedisPipeLine) HDel(key string, values ...string) {
	key = rp.r.addNamespacePrefix(key)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "HDEL", key)
		rp.log = append(rp.log, values...)
	}
	rp.pipeLine.HDel(context.Background(), key, values...)
}

func (rp *RedisPipeLine) XAdd(stream string, values []string) *PipeLineString {
	stream = rp.r.addNamespacePrefix(stream)
	rp.commands++
	if rp.r.engine.hasRedisLogger {
		rp.log = append(rp.log, "XADD", stream)
		rp.log = append(rp.log, values...)
	}
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec() {
	if rp.commands == 0 {
		return
	}
	start := getNow(rp.r.engine.hasRedisLogger)
	_, err := rp.pipeLine.Exec(context.Background())
	rp.pipeLine = rp.r.client.Pipeline()
	if err != nil && err == redis.Nil {
		err = nil
	}
	if rp.r.engine.hasRedisLogger {
		rp.fillLogFields(start, err)
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

func (rp *RedisPipeLine) fillLogFields(start *time.Time, err error) {
	query := strings.Join(rp.log, " ")
	fillLogFields(rp.r.engine, rp.r.engine.queryLoggersRedis, rp.pool, sourceRedis, "PIPELINE EXEC", query, start, false, err)
}
