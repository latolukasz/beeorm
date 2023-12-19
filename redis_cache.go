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

type RedisCache interface {
	Set(orm ORM, key string, value any, expiration time.Duration)
	MSet(orm ORM, pairs ...any)
	Del(orm ORM, keys ...string)
	HSet(orm ORM, key string, values ...any)
	HDel(orm ORM, key string, keys ...string)
	GetSet(orm ORM, key string, expiration time.Duration, provider func() any) any
	Info(orm ORM, section ...string) string
	GetConfig() RedisPoolConfig
	Get(orm ORM, key string) (value string, has bool)
	Eval(orm ORM, script string, keys []string, args ...any) any
	EvalSha(orm ORM, sha1 string, keys []string, args ...any) (res any, exists bool)
	SetNX(orm ORM, key string, value any, expiration time.Duration) bool
	ScriptExists(orm ORM, sha1 string) bool
	ScriptLoad(orm ORM, script string) string
	LPush(orm ORM, key string, values ...any) int64
	LPop(orm ORM, key string) string
	RPush(orm ORM, key string, values ...any) int64
	LLen(orm ORM, key string) int64
	Exists(orm ORM, keys ...string) int64
	Type(orm ORM, key string) string
	LRange(orm ORM, key string, start, stop int64) []string
	LSet(orm ORM, key string, index int64, value any)
	RPop(orm ORM, key string) (value string, found bool)
	BLMove(orm ORM, source, destination, srcPos, destPos string, timeout time.Duration) string
	LMove(orm ORM, source, destination, srcPos, destPos string) string
	LRem(orm ORM, key string, count int64, value any)
	Ltrim(orm ORM, key string, start, stop int64)
	HSetNx(orm ORM, key, field string, value any) bool
	HMGet(orm ORM, key string, fields ...string) map[string]any
	HGetAll(orm ORM, key string) map[string]string
	HGet(orm ORM, key, field string) (value string, has bool)
	HLen(orm ORM, key string) int64
	HIncrBy(orm ORM, key, field string, incr int64) int64
	IncrBy(orm ORM, key string, incr int64) int64
	Incr(orm ORM, key string) int64
	IncrWithExpire(orm ORM, key string, expire time.Duration) int64
	Expire(orm ORM, key string, expiration time.Duration) bool
	ZAdd(orm ORM, key string, members ...redis.Z) int64
	ZRevRange(orm ORM, key string, start, stop int64) []string
	ZRevRangeWithScores(orm ORM, key string, start, stop int64) []redis.Z
	ZRangeWithScores(orm ORM, key string, start, stop int64) []redis.Z
	ZCard(orm ORM, key string) int64
	ZCount(orm ORM, key string, min, max string) int64
	ZScore(orm ORM, key, member string) float64
	MGet(orm ORM, keys ...string) []any
	SAdd(orm ORM, key string, members ...any) int64
	SMembers(orm ORM, key string) []string
	SIsMember(orm ORM, key string, member any) bool
	SCard(orm ORM, key string) int64
	SPop(orm ORM, key string) (string, bool)
	SPopN(orm ORM, key string, max int64) []string
	XTrim(orm ORM, stream string, maxLen int64) (deleted int64)
	XRange(orm ORM, stream, start, stop string, count int64) []redis.XMessage
	XRevRange(orm ORM, stream, start, stop string, count int64) []redis.XMessage
	XInfoStream(orm ORM, stream string) *redis.XInfoStream
	XInfoGroups(orm ORM, stream string) []redis.XInfoGroup
	XGroupCreate(orm ORM, stream, group, start string) (key string, exists bool)
	XGroupCreateMkStream(orm ORM, stream, group, start string) (key string, exists bool)
	XGroupDestroy(orm ORM, stream, group string) int64
	XRead(orm ORM, a *redis.XReadArgs) []redis.XStream
	XDel(orm ORM, stream string, ids ...string) int64
	XGroupDelConsumer(orm ORM, stream, group, consumer string) int64
	XReadGroup(orm ORM, a *redis.XReadGroupArgs) (streams []redis.XStream)
	XPending(orm ORM, stream, group string) *redis.XPending
	XPendingExt(orm ORM, a *redis.XPendingExtArgs) []redis.XPendingExt
	XLen(orm ORM, stream string) int64
	XClaim(orm ORM, a *redis.XClaimArgs) []redis.XMessage
	XClaimJustID(orm ORM, a *redis.XClaimArgs) []string
	XAck(orm ORM, stream, group string, ids ...string) int64
	FlushAll(orm ORM)
	FlushDB(orm ORM)
	GetLocker() *Locker
	Process(orm ORM, cmd redis.Cmder) error
	GetCode() string
}

type redisCache struct {
	client *redis.Client
	locker *Locker
	config RedisPoolConfig
}

func (r *redisCache) GetSet(orm ORM, key string, expiration time.Duration, provider func() any) any {
	val, has := r.Get(orm, key)
	if !has {
		userVal := provider()
		encoded, _ := msgpack.Marshal(userVal)
		r.Set(orm, key, string(encoded), expiration)
		return userVal
	}
	var data any
	_ = msgpack.Unmarshal([]byte(val), &data)
	return data
}

func (r *redisCache) Info(orm ORM, section ...string) string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Info(orm.Context(), section...).Result()
	checkError(err)
	if hasLogger {
		message := "INFO"
		if len(section) > 0 {
			message += " " + strings.Join(section, " ")
		}
		r.fillLogFields(orm, "INFO", message, start, false, nil)
	}
	return val
}

func (r *redisCache) GetConfig() RedisPoolConfig {
	return r.config
}

func (r *redisCache) Get(orm ORM, key string) (value string, has bool) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Get(orm.Context(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(orm, "GET", "GET "+key, start, true, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(orm, "GET", "GET "+key, start, false, err)
	}
	return val, true
}

func (r *redisCache) Eval(orm ORM, script string, keys []string, args ...any) any {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.Eval(orm.Context(), script, keys, args...).Result()
	if hasLogger {
		message := fmt.Sprintf("EVAL "+script+" %v %v", keys, args)
		r.fillLogFields(orm, "EVAL", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) EvalSha(orm ORM, sha1 string, keys []string, args ...any) (res any, exists bool) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.EvalSha(orm.Context(), sha1, keys, args...).Result()
	if hasLogger {
		message := fmt.Sprintf("EVALSHA "+sha1+" %v %v", keys, args)
		r.fillLogFields(orm, "EVALSHA", message, start, false, err)
	}
	if err != nil && !r.ScriptExists(orm, sha1) {
		return nil, false
	}
	checkError(err)
	return res, true
}

func (r *redisCache) ScriptExists(orm ORM, sha1 string) bool {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.ScriptExists(orm.Context(), sha1).Result()
	if hasLogger {
		r.fillLogFields(orm, "SCRIPTEXISTS", "SCRIPTEXISTS "+sha1, start, false, err)
	}
	checkError(err)
	return res[0]
}

func (r *redisCache) ScriptLoad(orm ORM, script string) string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.ScriptLoad(orm.Context(), script).Result()
	if hasLogger {
		r.fillLogFields(orm, "SCRIPTLOAD", "SCRIPTLOAD "+script, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) Set(orm ORM, key string, value any, expiration time.Duration) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.Set(orm.Context(), key, value, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("SET %s %v %s", key, value, expiration)
		r.fillLogFields(orm, "SET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) SetNX(orm ORM, key string, value any, expiration time.Duration) bool {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	isSet, err := r.client.SetNX(orm.Context(), key, value, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("SET NX %s %v %s", key, value, expiration)
		r.fillLogFields(orm, "SETNX", message, start, false, err)
	}
	checkError(err)
	return isSet
}

func (r *redisCache) LPush(orm ORM, key string, values ...any) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LPush(orm.Context(), key, values...).Result()
	if hasLogger {
		message := "LPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(orm, "LPUSH", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LPop(orm ORM, key string) string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LPop(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "LPOP", "LPOP "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) RPush(orm ORM, key string, values ...any) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.RPush(orm.Context(), key, values...).Result()
	if hasLogger {
		message := "RPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(orm, "RPUSH", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LLen(orm ORM, key string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LLen(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "LLEN", "LLEN", start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Exists(orm ORM, keys ...string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Exists(orm.Context(), keys...).Result()
	if hasLogger {
		r.fillLogFields(orm, "EXISTS", "EXISTS "+strings.Join(keys, " "), start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Type(orm ORM, key string) string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Type(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "TYPE", "TYPE "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LRange(orm ORM, key string, start, stop int64) []string {
	hasLogger, _ := orm.getRedisLoggers()
	s := getNow(hasLogger)
	val, err := r.client.LRange(orm.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("LRANGE %s %d %d", key, start, stop)
		r.fillLogFields(orm, "LRANGE", message, s, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LSet(orm ORM, key string, index int64, value any) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.LSet(orm.Context(), key, index, value).Result()
	if hasLogger {
		message := fmt.Sprintf("LSET %s %d %v", key, index, value)
		r.fillLogFields(orm, "LSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) BLMove(orm ORM, source, destination, srcPos, destPos string, timeout time.Duration) string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	value, err := r.client.BLMove(orm.Context(), source, destination, srcPos, destPos, timeout).Result()
	if hasLogger {
		message := fmt.Sprintf("BLMOVE %s %s %s %s %s", source, destination, srcPos, destPos, timeout)
		r.fillLogFields(orm, "BLMOVE", message, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) LMove(orm ORM, source, destination, srcPos, destPos string) string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	value, err := r.client.LMove(orm.Context(), source, destination, srcPos, destPos).Result()
	if hasLogger {
		message := fmt.Sprintf("LMOVE %s %s %s %s", source, destination, srcPos, destPos)
		r.fillLogFields(orm, "LMOVE", message, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) RPop(orm ORM, key string) (value string, found bool) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.RPop(orm.Context(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(orm, "RPOP", "RPOP", start, false, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(orm, "RPOP", "RPOP", start, false, err)
	}
	return val, true
}

func (r *redisCache) LRem(orm ORM, key string, count int64, value any) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.LRem(orm.Context(), key, count, value).Result()
	if hasLogger {
		message := fmt.Sprintf("LREM %d %v", count, value)
		r.fillLogFields(orm, "LREM", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Ltrim(orm ORM, key string, start, stop int64) {
	hasLogger, _ := orm.getRedisLoggers()
	s := getNow(hasLogger)
	_, err := r.client.LTrim(orm.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("LTRIM %s %d %d", key, start, stop)
		r.fillLogFields(orm, "LTRIM", message, s, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSet(orm ORM, key string, values ...any) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.HSet(orm.Context(), key, values...).Result()
	if hasLogger {
		message := "HSET " + key + " "
		for _, v := range values {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(orm, "HSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSetNx(orm ORM, key, field string, value any) bool {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.HSetNX(orm.Context(), key, field, value).Result()
	if hasLogger {
		message := "HSETNX " + key + " " + field + " " + fmt.Sprintf(" %v", value)
		r.fillLogFields(orm, "HSETNX", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) HDel(orm ORM, key string, fields ...string) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.HDel(orm.Context(), key, fields...).Result()
	if hasLogger {
		message := "HDEL " + key + " " + strings.Join(fields, " ")
		r.fillLogFields(orm, "HDEL", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HMGet(orm ORM, key string, fields ...string) map[string]any {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HMGet(orm.Context(), key, fields...).Result()
	results := make(map[string]any, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if hasLogger {
		message := "HMGET " + key + " " + strings.Join(fields, " ")
		r.fillLogFields(orm, "HMGET", message, start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) HGetAll(orm ORM, key string) map[string]string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HGetAll(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "HGETALL", "HGETALL "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HGet(orm ORM, key, field string) (value string, has bool) {
	hasLogger, _ := orm.getRedisLoggers()
	misses := false
	start := getNow(hasLogger)
	val, err := r.client.HGet(orm.Context(), key, field).Result()
	if err == redis.Nil {
		err = nil
		misses = true
	}
	if hasLogger {
		r.fillLogFields(orm, "HGET", "HGET "+key+" "+field, start, misses, err)
	}
	checkError(err)
	return val, !misses
}

func (r *redisCache) HLen(orm ORM, key string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HLen(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "HLEN", "HLEN "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HIncrBy(orm ORM, key, field string, incr int64) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HIncrBy(orm.Context(), key, field, incr).Result()
	if hasLogger {
		message := fmt.Sprintf("HINCRBY %s %s %d", key, field, incr)
		r.fillLogFields(orm, "HINCRBY", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrBy(orm ORM, key string, incr int64) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.IncrBy(orm.Context(), key, incr).Result()
	if hasLogger {
		message := fmt.Sprintf("INCRBY %s %d", key, incr)
		r.fillLogFields(orm, "INCRBY", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Incr(orm ORM, key string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Incr(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "INCR", "INCR "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrWithExpire(orm ORM, key string, expire time.Duration) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	p := r.client.Pipeline()
	res := p.Incr(orm.Context(), key)
	p.Expire(orm.Context(), key, expire)
	_, err := p.Exec(orm.Context())
	if hasLogger {
		r.fillLogFields(orm, "INCR_EXPIRE", "INCR EXP "+key+" "+expire.String(), start, false, err)
	}
	checkError(err)
	value, err := res.Result()
	checkError(err)
	return value
}

func (r *redisCache) Expire(orm ORM, key string, expiration time.Duration) bool {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Expire(orm.Context(), key, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("EXPIRE %s %s", key, expiration.String())
		r.fillLogFields(orm, "EXPIRE", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZAdd(orm ORM, key string, members ...redis.Z) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZAdd(orm.Context(), key, members...).Result()
	if hasLogger {
		message := "ZADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %f %v", v.Score, v.Member)
		}
		r.fillLogFields(orm, "ZADD", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRange(orm ORM, key string, start, stop int64) []string {
	hasLogger, _ := orm.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRevRange(orm.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZREVRANGE %s %d %d", key, start, stop)
		r.fillLogFields(orm, "ZREVRANGE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRangeWithScores(orm ORM, key string, start, stop int64) []redis.Z {
	hasLogger, _ := orm.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRevRangeWithScores(orm.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZREVRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields(orm, "ZREVRANGESCORE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRangeWithScores(orm ORM, key string, start, stop int64) []redis.Z {
	hasLogger, _ := orm.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRangeWithScores(orm.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields(orm, "ZRANGESCORE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCard(orm ORM, key string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZCard(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "ZCARD", "ZCARD "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCount(orm ORM, key string, min, max string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZCount(orm.Context(), key, min, max).Result()
	if hasLogger {
		message := fmt.Sprintf("ZCOUNT %s %s %s", key, min, max)
		r.fillLogFields(orm, "ZCOUNT", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZScore(orm ORM, key, member string) float64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZScore(orm.Context(), key, member).Result()
	if hasLogger {
		message := fmt.Sprintf("ZSCORE %s %s", key, member)
		r.fillLogFields(orm, "ZSCORE", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) MSet(orm ORM, pairs ...any) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.MSet(orm.Context(), pairs...).Result()
	if hasLogger {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(orm, "MSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) MGet(orm ORM, keys ...string) []any {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.MGet(orm.Context(), keys...).Result()
	results := make([]any, len(keys))
	misses := 0
	for i, v := range val {
		results[i] = v
		if v == nil {
			misses++
		}
	}
	if hasLogger {
		r.fillLogFields(orm, "MGET", "MGET "+strings.Join(keys, " "), start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) SAdd(orm ORM, key string, members ...any) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SAdd(orm.Context(), key, members...).Result()
	if hasLogger {
		message := "SADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(orm, "SADD", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SMembers(orm ORM, key string) []string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SMembers(orm.Context(), key).Result()
	if hasLogger {
		message := "SMEMBERS " + key
		r.fillLogFields(orm, "SMEMBERS", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SIsMember(orm ORM, key string, member any) bool {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SIsMember(orm.Context(), key, member).Result()
	if hasLogger {
		r.fillLogFields(orm, "SISMEMBER", fmt.Sprintf("SISMEMBER %s %v", key, member), start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SCard(orm ORM, key string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SCard(orm.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(orm, "SCARD", "SCARD "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SPop(orm ORM, key string) (string, bool) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SPop(orm.Context(), key).Result()
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(orm, "SPOP", "SPOP "+key, start, false, err)
	}
	checkError(err)
	return val, found
}

func (r *redisCache) SPopN(orm ORM, key string, max int64) []string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SPopN(orm.Context(), key, max).Result()
	if hasLogger {
		message := fmt.Sprintf("SPOPN %s %d", key, max)
		r.fillLogFields(orm, "SPOPN", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Del(orm ORM, keys ...string) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.Del(orm.Context(), keys...).Result()
	if hasLogger {
		r.fillLogFields(orm, "DEL", "DEL "+strings.Join(keys, " "), start, false, err)
	}
	checkError(err)
}

func (r *redisCache) XTrim(orm ORM, stream string, maxLen int64) (deleted int64) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	var err error
	deleted, err = r.client.XTrimMaxLen(orm.Context(), stream, maxLen).Result()
	if hasLogger {
		message := fmt.Sprintf("XTREAM %s %d", stream, maxLen)
		r.fillLogFields(orm, "XTREAM", message, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRange(orm ORM, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := orm.getRedisLoggers()
	s := getNow(hasLogger)
	deleted, err := r.client.XRangeN(orm.Context(), stream, start, stop, count).Result()
	if hasLogger {
		message := fmt.Sprintf("XRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields(orm, "XTREAM", message, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRevRange(orm ORM, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := orm.getRedisLoggers()
	s := getNow(hasLogger)
	deleted, err := r.client.XRevRangeN(orm.Context(), stream, start, stop, count).Result()
	if hasLogger {
		message := fmt.Sprintf("XREVRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields(orm, "XREVRANGE", message, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XInfoStream(orm ORM, stream string) *redis.XInfoStream {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XInfoStream(orm.Context(), stream).Result()
	if hasLogger {
		r.fillLogFields(orm, "XINFOSTREAM", "XINFOSTREAM "+stream, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XInfoGroups(orm ORM, stream string) []redis.XInfoGroup {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XInfoGroups(orm.Context(), stream).Result()
	if err == redis.Nil {
		err = nil
	}
	if err != nil && err.Error() == "ERR no such key" {
		if hasLogger {
			r.fillLogFields(orm, "XINFOGROUPS", "XINFOGROUPS "+stream, start, false, err)
		}
		return make([]redis.XInfoGroup, 0)
	}
	if hasLogger {
		r.fillLogFields(orm, "XINFOGROUPS", "XINFOGROUPS "+stream, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XGroupCreate(orm ORM, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := orm.getRedisLoggers()
	s := getNow(hasLogger)
	res, err := r.client.XGroupCreate(orm.Context(), stream, group, start).Result()
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		if hasLogger {
			message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
			r.fillLogFields(orm, "XGROUPCREATE", message, s, false, err)
		}
		return "OK", true
	}
	if hasLogger {
		message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
		r.fillLogFields(orm, "XGROUPCREATE", message, s, false, err)
	}
	checkError(err)
	return res, false
}

func (r *redisCache) XGroupCreateMkStream(orm ORM, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := orm.getRedisLoggers()
	s := getNow(hasLogger)
	res, err := r.client.XGroupCreateMkStream(orm.Context(), stream, group, start).Result()
	created := false
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if hasLogger {
		message := fmt.Sprintf("XGROUPCRMKSM %s %s %s", stream, group, start)
		r.fillLogFields(orm, "XGROUPCREATEMKSTREAM", message, s, false, err)
	}
	checkError(err)
	return res, created
}

func (r *redisCache) XGroupDestroy(orm ORM, stream, group string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XGroupDestroy(orm.Context(), stream, group).Result()
	if hasLogger {
		message := fmt.Sprintf("XGROUPCDESTROY %s %s", stream, group)
		r.fillLogFields(orm, "XGROUPCDESTROY", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XRead(orm ORM, a *redis.XReadArgs) []redis.XStream {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XRead(orm.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XREAD %s COUNT %d BLOCK %d", strings.Join(a.Streams, " "), a.Count, a.Block)
		r.fillLogFields(orm, "XREAD", message, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XDel(orm ORM, stream string, ids ...string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	deleted, err := r.client.XDel(orm.Context(), stream, ids...).Result()
	if hasLogger {
		r.fillLogFields(orm, "XDEL", "XDEL "+stream+" "+strings.Join(ids, " "), start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XGroupDelConsumer(orm ORM, stream, group, consumer string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	deleted, err := r.client.XGroupDelConsumer(orm.Context(), stream, group, consumer).Result()
	if hasLogger {
		message := fmt.Sprintf("XGROUPDELCONSUMER %s %s %s", stream, group, consumer)
		r.fillLogFields(orm, "XGROUPDELCONSUMER", message, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XReadGroup(orm ORM, a *redis.XReadGroupArgs) (streams []redis.XStream) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	if hasLogger && a.Block >= 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d BLOCK %s NOACK %v", a.Count, a.Block.String(), a.NoAck)
		r.fillLogFields(orm, "XREADGROUP", message, start, false, nil)
	}

	var err error
	if a.Block >= 0 {
		ch := make(chan int)
		go func() {
			streams, err = r.client.XReadGroup(orm.Context(), a).Result()
			close(ch)
		}()
		select {
		case <-orm.Context().Done():
			return
		case <-ch:
			break
		}
	} else {
		streams, err = r.client.XReadGroup(orm.Context(), a).Result()
	}

	if err == redis.Nil {
		err = nil
	}
	if hasLogger && a.Block < 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d NOACK %v", a.Count, a.NoAck)
		r.fillLogFields(orm, "XREADGROUP", message, start, false, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	checkError(err)
	return streams
}

func (r *redisCache) XPending(orm ORM, stream, group string) *redis.XPending {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XPending(orm.Context(), stream, group).Result()
	if hasLogger {
		message := fmt.Sprintf("XPENDING %s %s", stream, group)
		r.fillLogFields(orm, "XPENDING", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XPendingExt(orm ORM, a *redis.XPendingExtArgs) []redis.XPendingExt {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XPendingExt(orm.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XPENDINGEXT %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" START %s END %s COUNT %d IDLE %s", a.Start, a.End, a.Count, a.Idle.String())
		r.fillLogFields(orm, "XPENDINGEXT", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XLen(orm ORM, stream string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	l, err := r.client.XLen(orm.Context(), stream).Result()
	if hasLogger {
		r.fillLogFields(orm, "XLEN", "XLEN "+stream, start, false, err)
	}
	checkError(err)
	return l
}

func (r *redisCache) XClaim(orm ORM, a *redis.XClaimArgs) []redis.XMessage {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XClaim(orm.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XCLAIM %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields(orm, "XCLAIM", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XClaimJustID(orm ORM, a *redis.XClaimArgs) []string {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XClaimJustID(orm.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XCLAIMJUSTID %s %s %s", a.Stream, a.Group, a.Consumer)

		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields(orm, "XCLAIMJUSTID", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XAck(orm ORM, stream, group string, ids ...string) int64 {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XAck(orm.Context(), stream, group, ids...).Result()
	if hasLogger {
		message := fmt.Sprintf("XACK %s %s %s", stream, group, strings.Join(ids, " "))
		r.fillLogFields(orm, "XACK", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) FlushAll(orm ORM) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FlushAll(orm.Context()).Result()
	if hasLogger {
		r.fillLogFields(orm, "FLUSHALL", "FLUSHALL", start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FlushDB(orm ORM) {
	hasLogger, _ := orm.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FlushDB(orm.Context()).Result()
	if hasLogger {
		r.fillLogFields(orm, "FLUSHDB", "FLUSHDB", start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Process(orm ORM, cmd redis.Cmder) error {
	return r.client.Process(orm.Context(), cmd)
}

func (r *redisCache) GetCode() string {
	return r.config.GetCode()
}

func (r *redisCache) fillLogFields(orm ORM, operation, query string, start *time.Time, cacheMiss bool, err error) {
	_, loggers := orm.getRedisLoggers()
	fillLogFields(orm, loggers, r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
