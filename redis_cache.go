package beeorm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shamaton/msgpack"
)

type RedisCacheSetter interface {
	Set(c Context, key string, value interface{}, expiration time.Duration)
	MSet(c Context, pairs ...interface{})
	Del(c Context, keys ...string)
	xAdd(c Context, stream string, values []string) (id string)
	HSet(c Context, key string, values ...interface{})
	HDel(c Context, key string, keys ...string)
}

type RedisCache interface {
	RedisCacheSetter
	GetSet(c Context, key string, expiration time.Duration, provider func() interface{}) interface{}
	Info(c Context, section ...string) string
	GetPoolConfig() RedisPoolConfig
	Get(c Context, key string) (value string, has bool)
	Eval(c Context, script string, keys []string, args ...interface{}) interface{}
	EvalSha(c Context, sha1 string, keys []string, args ...interface{}) (res interface{}, exists bool)
	SetNX(c Context, key string, value interface{}, expiration time.Duration) bool
	ScriptExists(c Context, sha1 string) bool
	ScriptLoad(c Context, script string) string
	LPush(c Context, key string, values ...interface{}) int64
	LPop(c Context, key string) string
	RPush(c Context, key string, values ...interface{}) int64
	LLen(c Context, key string) int64
	Exists(c Context, keys ...string) int64
	Type(c Context, key string) string
	LRange(c Context, key string, start, stop int64) []string
	LSet(c Context, key string, index int64, value interface{})
	RPop(c Context, key string) (value string, found bool)
	BLMove(c Context, source, destination, srcPos, destPos string, timeout time.Duration) string
	LMove(c Context, source, destination, srcPos, destPos string) string
	LRem(c Context, key string, count int64, value interface{})
	Ltrim(c Context, key string, start, stop int64)
	HSetNx(c Context, key, field string, value interface{}) bool
	hDelUints(c Context, key string, fields ...uint64)
	HMGet(c Context, key string, fields ...string) map[string]interface{}
	hMGetUints(c Context, key string, fields ...uint64) []interface{}
	HGetAll(c Context, key string) map[string]string
	HGet(c Context, key, field string) (value string, has bool)
	HLen(c Context, key string) int64
	HIncrBy(c Context, key, field string, incr int64) int64
	IncrBy(c Context, key string, incr int64) int64
	Incr(c Context, key string) int64
	IncrWithExpire(c Context, key string, expire time.Duration) int64
	Expire(c Context, key string, expiration time.Duration) bool
	ZAdd(c Context, key string, members ...redis.Z) int64
	ZRevRange(c Context, key string, start, stop int64) []string
	ZRevRangeWithScores(c Context, key string, start, stop int64) []redis.Z
	ZRangeWithScores(c Context, key string, start, stop int64) []redis.Z
	ZCard(c Context, key string) int64
	ZCount(c Context, key string, min, max string) int64
	ZScore(c Context, key, member string) float64
	MGet(c Context, keys ...string) []interface{}
	SAdd(c Context, key string, members ...interface{}) int64
	SCard(c Context, key string) int64
	SPop(c Context, key string) (string, bool)
	SPopN(c Context, key string, max int64) []string
	XTrim(c Context, stream string, maxLen int64) (deleted int64)
	XRange(c Context, stream, start, stop string, count int64) []redis.XMessage
	XRevRange(c Context, stream, start, stop string, count int64) []redis.XMessage
	XInfoStream(c Context, stream string) *redis.XInfoStream
	XInfoGroups(c Context, stream string) []redis.XInfoGroup
	XGroupCreate(c Context, stream, group, start string) (key string, exists bool)
	XGroupCreateMkStream(c Context, stream, group, start string) (key string, exists bool)
	XGroupDestroy(c Context, stream, group string) int64
	XRead(c Context, a *redis.XReadArgs) []redis.XStream
	XDel(c Context, stream string, ids ...string) int64
	XGroupDelConsumer(c Context, stream, group, consumer string) int64
	XReadGroup(c Context, a *redis.XReadGroupArgs) (streams []redis.XStream)
	XPending(c Context, stream, group string) *redis.XPending
	XPendingExt(c Context, a *redis.XPendingExtArgs) []redis.XPendingExt
	XLen(c Context, stream string) int64
	XClaim(c Context, a *redis.XClaimArgs) []redis.XMessage
	XClaimJustID(c Context, a *redis.XClaimArgs) []string
	XAck(c Context, stream, group string, ids ...string) int64
	FlushAll(c Context)
	FlushDB(c Context)
	GetLocker() *Locker
	Process(c Context, cmd redis.Cmder) error
	GetCode() string
}

type redisCache struct {
	client *redis.Client
	locker *Locker
	config RedisPoolConfig
}

func (r *redisCache) GetSet(c Context, key string, expiration time.Duration, provider func() interface{}) interface{} {
	val, has := r.Get(c, key)
	if !has {
		userVal := provider()
		encoded, _ := msgpack.Marshal(userVal)
		r.Set(c, key, string(encoded), expiration)
		return userVal
	}
	var data interface{}
	_ = msgpack.Unmarshal([]byte(val), &data)
	return data
}

func (r *redisCache) Info(c Context, section ...string) string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Info(c.Ctx(), section...).Result()
	checkError(err)
	if hasLogger {
		message := "INFO"
		if len(section) > 0 {
			message += " " + strings.Join(section, " ")
		}
		r.fillLogFields(c, "INFO", message, start, false, nil)
	}
	return val
}

func (r *redisCache) GetPoolConfig() RedisPoolConfig {
	return r.config
}

func (r *redisCache) Get(c Context, key string) (value string, has bool) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Get(c.Ctx(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(c, "GET", "GET "+key, start, true, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(c, "GET", "GET "+key, start, false, err)
	}
	return val, true
}

func (r *redisCache) Eval(c Context, script string, keys []string, args ...interface{}) interface{} {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.Eval(c.Ctx(), script, keys, args...).Result()
	if hasLogger {
		message := fmt.Sprintf("EVAL "+script+" %v %v", keys, args)
		r.fillLogFields(c, "EVAL", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) EvalSha(c Context, sha1 string, keys []string, args ...interface{}) (res interface{}, exists bool) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.EvalSha(c.Ctx(), sha1, keys, args...).Result()
	if hasLogger {
		message := fmt.Sprintf("EVALSHA "+sha1+" %v %v", keys, args)
		r.fillLogFields(c, "EVALSHA", message, start, false, err)
	}
	if err != nil && !r.ScriptExists(c, sha1) {
		return nil, false
	}
	checkError(err)
	return res, true
}

func (r *redisCache) ScriptExists(c Context, sha1 string) bool {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.ScriptExists(c.Ctx(), sha1).Result()
	if hasLogger {
		r.fillLogFields(c, "SCRIPTEXISTS", "SCRIPTEXISTS "+sha1, start, false, err)
	}
	checkError(err)
	return res[0]
}

func (r *redisCache) ScriptLoad(c Context, script string) string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.ScriptLoad(c.Ctx(), script).Result()
	if hasLogger {
		r.fillLogFields(c, "SCRIPTLOAD", "SCRIPTLOAD "+script, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) Set(c Context, key string, value interface{}, expiration time.Duration) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.Set(c.Ctx(), key, value, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("SET %s %v %s", key, value, expiration)
		r.fillLogFields(c, "SET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) SetNX(c Context, key string, value interface{}, expiration time.Duration) bool {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	isSet, err := r.client.SetNX(c.Ctx(), key, value, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("SET NX %s %v %s", key, value, expiration)
		r.fillLogFields(c, "SETNX", message, start, false, err)
	}
	checkError(err)
	return isSet
}

func (r *redisCache) LPush(c Context, key string, values ...interface{}) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LPush(c.Ctx(), key, values...).Result()
	if hasLogger {
		message := "LPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(c, "LPUSH", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LPop(c Context, key string) string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LPop(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "LPOP", "LPOP "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) RPush(c Context, key string, values ...interface{}) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.RPush(c.Ctx(), key, values...).Result()
	if hasLogger {
		message := "RPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(c, "RPUSH", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LLen(c Context, key string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LLen(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "LLEN", "LLEN", start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Exists(c Context, keys ...string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Exists(c.Ctx(), keys...).Result()
	if hasLogger {
		r.fillLogFields(c, "EXISTS", "EXISTS "+strings.Join(keys, " "), start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Type(c Context, key string) string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Type(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "TYPE", "TYPE "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LRange(c Context, key string, start, stop int64) []string {
	hasLogger, _ := c.getRedisLoggers()
	s := getNow(hasLogger)
	val, err := r.client.LRange(c.Ctx(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("LRANGE %s %d %d", key, start, stop)
		r.fillLogFields(c, "LRANGE", message, s, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LSet(c Context, key string, index int64, value interface{}) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.LSet(c.Ctx(), key, index, value).Result()
	if hasLogger {
		message := fmt.Sprintf("LSET %d %v", index, value)
		r.fillLogFields(c, "LSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) BLMove(c Context, source, destination, srcPos, destPos string, timeout time.Duration) string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	value, err := r.client.BLMove(c.Ctx(), source, destination, srcPos, destPos, timeout).Result()
	if hasLogger {
		message := fmt.Sprintf("BLMOVE %s %s %s %s %s", source, destination, srcPos, destPos, timeout)
		r.fillLogFields(c, "BLMOVE", message, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) LMove(c Context, source, destination, srcPos, destPos string) string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	value, err := r.client.LMove(c.Ctx(), source, destination, srcPos, destPos).Result()
	if hasLogger {
		message := fmt.Sprintf("LMOVE %s %s %s %s", source, destination, srcPos, destPos)
		r.fillLogFields(c, "LMOVE", message, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) RPop(c Context, key string) (value string, found bool) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.RPop(c.Ctx(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(c, "RPOP", "RPOP", start, false, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(c, "RPOP", "RPOP", start, false, err)
	}
	return val, true
}

func (r *redisCache) LRem(c Context, key string, count int64, value interface{}) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.LRem(c.Ctx(), key, count, value).Result()
	if hasLogger {
		message := fmt.Sprintf("LREM %d %v", count, value)
		r.fillLogFields(c, "LREM", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Ltrim(c Context, key string, start, stop int64) {
	hasLogger, _ := c.getRedisLoggers()
	s := getNow(hasLogger)
	_, err := r.client.LTrim(c.Ctx(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("LTRIM %s %d %d", key, start, stop)
		r.fillLogFields(c, "LTRIM", message, s, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSet(c Context, key string, values ...interface{}) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.HSet(c.Ctx(), key, values...).Result()
	if hasLogger {
		message := "HSET " + key + " "
		for _, v := range values {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(c, "HSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSetNx(c Context, key, field string, value interface{}) bool {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.HSetNX(c.Ctx(), key, field, value).Result()
	if hasLogger {
		message := "HSETNX " + key + " " + field + " " + fmt.Sprintf(" %v", value)
		r.fillLogFields(c, "HSETNX", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) HDel(c Context, key string, fields ...string) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.HDel(c.Ctx(), key, fields...).Result()
	if hasLogger {
		message := "HDEL " + key + " " + strings.Join(fields, " ")
		r.fillLogFields(c, "HDEL", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) hDelUints(c Context, key string, fields ...uint64) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	args := make([]interface{}, 2+len(fields))
	args[0] = "hdel"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := redis.NewIntCmd(c.Ctx(), args...)
	err := r.client.Process(c.Ctx(), cmd)
	if hasLogger {
		message := fmt.Sprintf("HDEL %s %v", key, fields)
		r.fillLogFields(c, "HDEL", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HMGet(c Context, key string, fields ...string) map[string]interface{} {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HMGet(c.Ctx(), key, fields...).Result()
	results := make(map[string]interface{}, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if hasLogger {
		message := "HMGET " + key + " " + strings.Join(fields, " ")
		r.fillLogFields(c, "HMGET", message, start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) hMGetUints(c Context, key string, fields ...uint64) []interface{} {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)

	args := make([]interface{}, 2+len(fields))
	args[0] = "hmget"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := redis.NewSliceCmd(c.Ctx(), args...)
	err := r.client.Process(c.Ctx(), cmd)
	misses := false
	var results []interface{}
	if err == nil {
		results, err = cmd.Result()
		if err == nil {
			if len(results) == 0 {
				results = make([]interface{}, len(fields))
			}
			if hasLogger {
				for _, v := range results {
					if v == nil {
						misses = true
						break
					}
				}
			}
		}
	}
	if hasLogger {
		message := "HMGET " + key + " " + fmt.Sprintf("%v", fields)
		r.fillLogFields(c, "HMGET", message, start, misses, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) HGetAll(c Context, key string) map[string]string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HGetAll(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "HGETALL", "HGETALL "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HGet(c Context, key, field string) (value string, has bool) {
	hasLogger, _ := c.getRedisLoggers()
	misses := false
	start := getNow(hasLogger)
	val, err := r.client.HGet(c.Ctx(), key, field).Result()
	if err == redis.Nil {
		err = nil
		misses = true
	}
	if hasLogger {
		r.fillLogFields(c, "HGET", "HGET "+key+" "+field, start, misses, err)
	}
	checkError(err)
	return val, !misses
}

func (r *redisCache) HLen(c Context, key string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HLen(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "HLEN", "HLEN "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HIncrBy(c Context, key, field string, incr int64) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HIncrBy(c.Ctx(), key, field, incr).Result()
	if hasLogger {
		message := fmt.Sprintf("HINCRBY %s %s %d", key, field, incr)
		r.fillLogFields(c, "HINCRBY", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrBy(c Context, key string, incr int64) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.IncrBy(c.Ctx(), key, incr).Result()
	if hasLogger {
		message := fmt.Sprintf("INCRBY %s %d", key, incr)
		r.fillLogFields(c, "INCRBY", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Incr(c Context, key string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Incr(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "INCR", "INCR "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrWithExpire(c Context, key string, expire time.Duration) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	p := r.client.Pipeline()
	res := p.Incr(c.Ctx(), key)
	p.Expire(c.Ctx(), key, expire)
	_, err := p.Exec(c.Ctx())
	if hasLogger {
		r.fillLogFields(c, "INCR_EXPIRE", "INCR EXP "+key+" "+expire.String(), start, false, err)
	}
	checkError(err)
	value, err := res.Result()
	checkError(err)
	return value
}

func (r *redisCache) Expire(c Context, key string, expiration time.Duration) bool {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Expire(c.Ctx(), key, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("EXPIRE %s %s", key, expiration.String())
		r.fillLogFields(c, "EXPIRE", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZAdd(c Context, key string, members ...redis.Z) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZAdd(c.Ctx(), key, members...).Result()
	if hasLogger {
		message := "ZADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %f %v", v.Score, v.Member)
		}
		r.fillLogFields(c, "ZADD", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRange(c Context, key string, start, stop int64) []string {
	hasLogger, _ := c.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRevRange(c.Ctx(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZREVRANGE %s %d %d", key, start, stop)
		r.fillLogFields(c, "ZREVRANGE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRangeWithScores(c Context, key string, start, stop int64) []redis.Z {
	hasLogger, _ := c.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRevRangeWithScores(c.Ctx(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZREVRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields(c, "ZREVRANGESCORE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRangeWithScores(c Context, key string, start, stop int64) []redis.Z {
	hasLogger, _ := c.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRangeWithScores(c.Ctx(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields(c, "ZRANGESCORE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCard(c Context, key string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZCard(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "ZCARD", "ZCARD "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCount(c Context, key string, min, max string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZCount(c.Ctx(), key, min, max).Result()
	if hasLogger {
		message := fmt.Sprintf("ZCOUNT %s %s %s", key, min, max)
		r.fillLogFields(c, "ZCOUNT", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZScore(c Context, key, member string) float64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZScore(c.Ctx(), key, member).Result()
	if hasLogger {
		message := fmt.Sprintf("ZSCORE %s %s", key, member)
		r.fillLogFields(c, "ZSCORE", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) MSet(c Context, pairs ...interface{}) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.MSet(c.Ctx(), pairs...).Result()
	if hasLogger {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(c, "MSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) MGet(c Context, keys ...string) []interface{} {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.MGet(c.Ctx(), keys...).Result()
	results := make([]interface{}, len(keys))
	misses := 0
	for i, v := range val {
		results[i] = v
		if v == nil {
			misses++
		}
	}
	if hasLogger {
		r.fillLogFields(c, "MGET", "MGET "+strings.Join(keys, " "), start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) SAdd(c Context, key string, members ...interface{}) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SAdd(c.Ctx(), key, members...).Result()
	if hasLogger {
		message := "SADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(c, "SADD", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SCard(c Context, key string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SCard(c.Ctx(), key).Result()
	if hasLogger {
		r.fillLogFields(c, "SCARD", "SCARD "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SPop(c Context, key string) (string, bool) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SPop(c.Ctx(), key).Result()
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(c, "SPOP", "SPOP "+key, start, false, err)
	}
	checkError(err)
	return val, found
}

func (r *redisCache) SPopN(c Context, key string, max int64) []string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SPopN(c.Ctx(), key, max).Result()
	if hasLogger {
		message := fmt.Sprintf("SPOPN %s %d", key, max)
		r.fillLogFields(c, "SPOPN", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Del(c Context, keys ...string) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.Del(c.Ctx(), keys...).Result()
	if hasLogger {
		r.fillLogFields(c, "DEL", "DEL "+strings.Join(keys, " "), start, false, err)
	}
	checkError(err)
}

func (r *redisCache) XTrim(c Context, stream string, maxLen int64) (deleted int64) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	var err error
	deleted, err = r.client.XTrimMaxLen(c.Ctx(), stream, maxLen).Result()
	if hasLogger {
		message := fmt.Sprintf("XTREAM %s %d", stream, maxLen)
		r.fillLogFields(c, "XTREAM", message, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRange(c Context, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := c.getRedisLoggers()
	s := getNow(hasLogger)
	deleted, err := r.client.XRangeN(c.Ctx(), stream, start, stop, count).Result()
	if hasLogger {
		message := fmt.Sprintf("XRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields(c, "XTREAM", message, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRevRange(c Context, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := c.getRedisLoggers()
	s := getNow(hasLogger)
	deleted, err := r.client.XRevRangeN(c.Ctx(), stream, start, stop, count).Result()
	if hasLogger {
		message := fmt.Sprintf("XREVRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields(c, "XREVRANGE", message, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XInfoStream(c Context, stream string) *redis.XInfoStream {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XInfoStream(c.Ctx(), stream).Result()
	if hasLogger {
		r.fillLogFields(c, "XINFOSTREAM", "XINFOSTREAM "+stream, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XInfoGroups(c Context, stream string) []redis.XInfoGroup {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XInfoGroups(c.Ctx(), stream).Result()
	if err == redis.Nil {
		err = nil
	}
	if err != nil && err.Error() == "ERR no such key" {
		if hasLogger {
			r.fillLogFields(c, "XINFOGROUPS", "XINFOGROUPS "+stream, start, false, err)
		}
		return make([]redis.XInfoGroup, 0)
	}
	if hasLogger {
		r.fillLogFields(c, "XINFOGROUPS", "XINFOGROUPS "+stream, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XGroupCreate(c Context, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := c.getRedisLoggers()
	s := getNow(hasLogger)
	res, err := r.client.XGroupCreate(c.Ctx(), stream, group, start).Result()
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		if hasLogger {
			message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
			r.fillLogFields(c, "XGROUPCREATE", message, s, false, err)
		}
		return "OK", true
	}
	if hasLogger {
		message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
		r.fillLogFields(c, "XGROUPCREATE", message, s, false, err)
	}
	checkError(err)
	return res, false
}

func (r *redisCache) XGroupCreateMkStream(c Context, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := c.getRedisLoggers()
	s := getNow(hasLogger)
	res, err := r.client.XGroupCreateMkStream(c.Ctx(), stream, group, start).Result()
	created := false
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if hasLogger {
		message := fmt.Sprintf("XGROUPCRMKSM %s %s %s", stream, group, start)
		r.fillLogFields(c, "XGROUPCREATEMKSTREAM", message, s, false, err)
	}
	checkError(err)
	return res, created
}

func (r *redisCache) XGroupDestroy(c Context, stream, group string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XGroupDestroy(c.Ctx(), stream, group).Result()
	if hasLogger {
		message := fmt.Sprintf("XGROUPCDESTROY %s %s", stream, group)
		r.fillLogFields(c, "XGROUPCDESTROY", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XRead(c Context, a *redis.XReadArgs) []redis.XStream {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XRead(c.Ctx(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XREAD %s COUNT %d BLOCK %d", strings.Join(a.Streams, " "), a.Count, a.Block)
		r.fillLogFields(c, "XREAD", message, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XDel(c Context, stream string, ids ...string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	deleted, err := r.client.XDel(c.Ctx(), stream, ids...).Result()
	if hasLogger {
		r.fillLogFields(c, "XDEL", "XDEL "+stream+" "+strings.Join(ids, " "), start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XGroupDelConsumer(c Context, stream, group, consumer string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	deleted, err := r.client.XGroupDelConsumer(c.Ctx(), stream, group, consumer).Result()
	if hasLogger {
		message := fmt.Sprintf("XGROUPDELCONSUMER %s %s %s", stream, group, consumer)
		r.fillLogFields(c, "XGROUPDELCONSUMER", message, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XReadGroup(c Context, a *redis.XReadGroupArgs) (streams []redis.XStream) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	if hasLogger && a.Block >= 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d BLOCK %s NOACK %v", a.Count, a.Block.String(), a.NoAck)
		r.fillLogFields(c, "XREADGROUP", message, start, false, nil)
	}

	var err error
	if a.Block >= 0 {
		ch := make(chan int)
		go func() {
			streams, err = r.client.XReadGroup(c.Ctx(), a).Result()
			close(ch)
		}()
		select {
		case <-c.Ctx().Done():
			return
		case <-ch:
			break
		}
	} else {
		streams, err = r.client.XReadGroup(c.Ctx(), a).Result()
	}

	if err == redis.Nil {
		err = nil
	}
	if hasLogger && a.Block < 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d NOACK %v", a.Count, a.NoAck)
		r.fillLogFields(c, "XREADGROUP", message, start, false, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	checkError(err)
	return streams
}

func (r *redisCache) XPending(c Context, stream, group string) *redis.XPending {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XPending(c.Ctx(), stream, group).Result()
	if hasLogger {
		message := fmt.Sprintf("XPENDING %s %s", stream, group)
		r.fillLogFields(c, "XPENDING", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XPendingExt(c Context, a *redis.XPendingExtArgs) []redis.XPendingExt {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XPendingExt(c.Ctx(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XPENDINGEXT %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" START %s END %s COUNT %d IDLE %s", a.Start, a.End, a.Count, a.Idle.String())
		r.fillLogFields(c, "XPENDINGEXT", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) xAdd(c Context, stream string, values []string) (id string) {
	hasLogger, _ := c.getRedisLoggers()
	a := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	start := getNow(hasLogger)
	id, err := r.client.XAdd(c.Ctx(), a).Result()
	if hasLogger {
		message := "XADD " + stream + " " + strings.Join(values, " ")
		r.fillLogFields(c, "XADD", message, start, false, err)
	}
	checkError(err)
	return id
}

func (r *redisCache) XLen(c Context, stream string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	l, err := r.client.XLen(c.Ctx(), stream).Result()
	if hasLogger {
		r.fillLogFields(c, "XLEN", "XLEN "+stream, start, false, err)
	}
	checkError(err)
	return l
}

func (r *redisCache) XClaim(c Context, a *redis.XClaimArgs) []redis.XMessage {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XClaim(c.Ctx(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XCLAIM %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields(c, "XCLAIM", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XClaimJustID(c Context, a *redis.XClaimArgs) []string {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XClaimJustID(c.Ctx(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XCLAIMJUSTID %s %s %s", a.Stream, a.Group, a.Consumer)

		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields(c, "XCLAIMJUSTID", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XAck(c Context, stream, group string, ids ...string) int64 {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XAck(c.Ctx(), stream, group, ids...).Result()
	if hasLogger {
		message := fmt.Sprintf("XACK %s %s %s", stream, group, strings.Join(ids, " "))
		r.fillLogFields(c, "XACK", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) FlushAll(c Context) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FlushAll(c.Ctx()).Result()
	if hasLogger {
		r.fillLogFields(c, "FLUSHALL", "FLUSHALL", start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FlushDB(c Context) {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FlushDB(c.Ctx()).Result()
	if hasLogger {
		r.fillLogFields(c, "FLUSHDB", "FLUSHDB", start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Process(c Context, cmd redis.Cmder) error {
	return r.client.Process(c.Ctx(), cmd)
}

func (r *redisCache) GetCode() string {
	return r.config.GetCode()
}

func (r *redisCache) fillLogFields(c Context, operation, query string, start *time.Time, cacheMiss bool, err error) {
	_, loggers := c.getRedisLoggers()
	fillLogFields(c, loggers, r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
