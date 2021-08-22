package beeorm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shamaton/msgpack"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis_rate/v9"
)

type RedisCache struct {
	engine  *Engine
	client  *redis.Client
	limiter *redis_rate.Limiter
	locker  *Locker
	config  RedisPoolConfig
}

func (r *RedisCache) RateLimit(key string, period time.Duration, limit int) bool {
	if r.limiter == nil {
		r.limiter = redis_rate.NewLimiter(r.client)
	}
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.limiter.Allow(r.client.Context(), key, redis_rate.Limit{
		Rate:   limit,
		Period: period,
		Burst:  limit,
	})
	if r.engine.hasRedisLogger {
		r.fillLogFields("RATE", "RATE "+key+" "+period.String(), start, err)
	}
	checkError(err)
	return res.Allowed > 0
}

func (r *RedisCache) GetSet(key string, ttlSeconds int, provider func() interface{}) interface{} {
	val, has := r.Get(key)
	if !has {
		userVal := provider()
		encoded, _ := msgpack.Marshal(userVal)
		r.Set(key, string(encoded), ttlSeconds)
		return userVal
	}
	var data interface{}
	_ = msgpack.Unmarshal([]byte(val), &data)
	return data
}

func (r *RedisCache) PipeLine() *RedisPipeLine {
	return &RedisPipeLine{ctx: r.client.Context(), pool: r.config.GetCode(), r: r, pipeLine: r.client.Pipeline()}
}

func (r *RedisCache) Info(section ...string) string {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.Info(context.Background(), section...).Result()
	checkError(err)
	if r.engine.hasRedisLogger {
		message := "INFO"
		if len(section) > 0 {
			message += " " + strings.Join(section, " ")
		}
		r.fillLogFields("INFO", message, start, nil)
	}
	return val
}

func (r *RedisCache) GetPoolConfig() RedisPoolConfig {
	return r.config
}

func (r *RedisCache) Get(key string) (value string, has bool) {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.Get(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if r.engine.hasRedisLogger {
			r.fillLogFields("GET", "GET "+key, start, err)
		}
		checkError(err)
		return "", false
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("GET", "GET "+key, start, err)
	}
	return val, true
}

func (r *RedisCache) Eval(script string, keys []string, args ...interface{}) interface{} {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.Eval(context.Background(), script, keys, args...).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("EVAL "+script+" %v %v", keys, args)
		r.fillLogFields("EVAL", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) EvalSha(sha1 string, keys []string, args ...interface{}) interface{} {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.EvalSha(context.Background(), sha1, keys, args...).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("EVALSHA "+sha1+" %v %v", keys, args)
		r.fillLogFields("EVALSHA", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) ScriptLoad(script string) string {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.ScriptLoad(context.Background(), script).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("SCRIPTLOAD", "SCRIPTLOAD "+script, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) Set(key string, value interface{}, ttlSeconds int) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.Set(context.Background(), key, value, time.Duration(ttlSeconds)*time.Second).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("SET %s %v %d", key, value, ttlSeconds)
		r.fillLogFields("SET", message, start, err)
	}
	checkError(err)
}

func (r *RedisCache) SetNX(key string, value interface{}, ttlSeconds int) bool {
	start := getNow(r.engine.hasRedisLogger)
	isSet, err := r.client.SetNX(context.Background(), key, value, time.Duration(ttlSeconds)*time.Second).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("SET NX %s %v %d", key, value, ttlSeconds)
		r.fillLogFields("SETNX", message, start, err)
	}
	checkError(err)
	return isSet
}

func (r *RedisCache) LPush(key string, values ...interface{}) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.LPush(context.Background(), key, values...).Result()
	if r.engine.hasRedisLogger {
		message := "LPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields("LPUSH", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) RPush(key string, values ...interface{}) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.RPush(context.Background(), key, values...).Result()
	if r.engine.hasRedisLogger {
		message := "RPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields("RPUSH", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) LLen(key string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.LLen(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("LLEN", "LLEN", start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Exists(keys ...string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.Exists(context.Background(), keys...).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("EXISTS", "EXISTS "+strings.Join(keys, " "), start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Type(key string) string {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.Type(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("TYPE", "TYPE "+key, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) LRange(key string, start, stop int64) []string {
	s := getNow(r.engine.hasRedisLogger)
	val, err := r.client.LRange(context.Background(), key, start, stop).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("LRANGE %d %d", start, stop)
		r.fillLogFields("LRANGE", message, s, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) LSet(key string, index int64, value interface{}) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.LSet(context.Background(), key, index, value).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("LSET %d %v", index, value)
		r.fillLogFields("LSET", message, start, err)
	}
	checkError(err)
}

func (r *RedisCache) RPop(key string) (value string, found bool) {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.RPop(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if r.engine.hasRedisLogger {
			r.fillLogFields("RPOP", "RPOP", start, err)
		}
		checkError(err)
		return "", false
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("RPOP", "RPOP", start, err)
	}
	return val, true
}

func (r *RedisCache) LRem(key string, count int64, value interface{}) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.LRem(context.Background(), key, count, value).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("LREM %d %v", count, value)
		r.fillLogFields("LREM", message, start, err)
	}
	checkError(err)
}

func (r *RedisCache) Ltrim(key string, start, stop int64) {
	s := getNow(r.engine.hasRedisLogger)
	_, err := r.client.LTrim(context.Background(), key, start, stop).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("LTRIM %d %d", start, stop)
		r.fillLogFields("LTRIM", message, s, err)
	}
	checkError(err)
}

func (r *RedisCache) HSet(key string, values ...interface{}) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.HSet(context.Background(), key, values...).Result()
	if r.engine.hasRedisLogger {
		message := "HSET " + key + " "
		for _, v := range values {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields("HSET", message, start, err)
	}
	checkError(err)
}

func (r *RedisCache) HSetNx(key, field string, value interface{}) bool {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.HSetNX(context.Background(), key, field, value).Result()
	if r.engine.hasRedisLogger {
		message := "HSETNX " + key + " " + field + " " + fmt.Sprintf(" %v", value)
		r.fillLogFields("HSETNX", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) HDel(key string, fields ...string) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.HDel(context.Background(), key, fields...).Result()
	if r.engine.hasRedisLogger {
		message := "HDEL " + key + " " + strings.Join(fields, " ")
		r.fillLogFields("HDEL", message, start, err)
	}
	checkError(err)
}

func (r *RedisCache) HMGet(key string, fields ...string) map[string]interface{} {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.HMGet(context.Background(), key, fields...).Result()
	results := make(map[string]interface{}, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if r.engine.hasRedisLogger {
		message := "HMGET " + key + " " + strings.Join(fields, " ")
		r.fillLogFields("HMGET", message, start, err)
	}
	return results
}

func (r *RedisCache) HGetAll(key string) map[string]string {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.HGetAll(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("HGETALL", "HGETALL "+key, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) HGet(key, field string) (value string, has bool) {
	misses := 0
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.HGet(context.Background(), key, field).Result()
	if err == redis.Nil {
		err = nil
		misses = 1
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("HGET", "HGET "+key+" "+field, start, err)
	}
	checkError(err)
	return val, misses == 0
}

func (r *RedisCache) HLen(key string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.HLen(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("HLEN", "HLEN "+key, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) HIncrBy(key, field string, incr int64) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.HIncrBy(context.Background(), key, field, incr).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("HINCRBY %s %s %d", key, field, incr)
		r.fillLogFields("HINCRBY", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) IncrBy(key string, incr int64) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.IncrBy(context.Background(), key, incr).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("INCRBY %s %d", key, incr)
		r.fillLogFields("INCRBY", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Incr(key string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.Incr(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("INCR", "INCR "+key, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Expire(key string, expiration time.Duration) bool {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.Expire(context.Background(), key, expiration).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("EXPIRE %s %s", key, expiration.String())
		r.fillLogFields("EXPIRE", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZAdd(key string, members ...*redis.Z) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZAdd(context.Background(), key, members...).Result()
	if r.engine.hasRedisLogger {
		message := "ZADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %f %v", v.Score, v.Member)
		}
		r.fillLogFields("ZADD", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZRevRange(key string, start, stop int64) []string {
	startTime := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZRevRange(context.Background(), key, start, stop).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("ZREVRANGE %s %d %d", key, start, stop)
		r.fillLogFields("ZREVRANGE", message, startTime, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZRevRangeWithScores(key string, start, stop int64) []redis.Z {
	startTime := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZRevRangeWithScores(context.Background(), key, start, stop).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("ZREVRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields("ZREVRANGESCORE", message, startTime, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZRangeWithScores(key string, start, stop int64) []redis.Z {
	startTime := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZRangeWithScores(context.Background(), key, start, stop).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("ZRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields("ZRANGESCORE", message, startTime, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZCard(key string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZCard(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("ZCARD", "ZCARD "+key, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZCount(key string, min, max string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZCount(context.Background(), key, min, max).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("ZCOUNT %s %s %s", key, min, max)
		r.fillLogFields("ZCOUNT", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZScore(key, member string) float64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.ZScore(context.Background(), key, member).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("ZSCORE %s %s", key, member)
		r.fillLogFields("ZSCORE", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) MSet(pairs ...interface{}) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.MSet(context.Background(), pairs...).Result()
	if r.engine.hasRedisLogger {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields("MSET", message, start, err)
	}
	checkError(err)
}

func (r *RedisCache) MGet(keys ...string) []interface{} {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.MGet(context.Background(), keys...).Result()
	results := make([]interface{}, len(keys))
	misses := 0
	for i, v := range val {
		results[i] = v
		if v == nil {
			misses++
		}
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("MGET", "MGET "+strings.Join(keys, " "), start, err)
	}
	checkError(err)
	return results
}

func (r *RedisCache) SAdd(key string, members ...interface{}) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.SAdd(context.Background(), key, members...).Result()
	if r.engine.hasRedisLogger {
		message := "SADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields("SADD", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) SCard(key string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.SCard(context.Background(), key).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("SCARD", "SCARD "+key, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) SPop(key string) (string, bool) {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.SPop(context.Background(), key).Result()
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("SPOP", "SPOP "+key, start, err)
	}
	checkError(err)
	return val, found
}

func (r *RedisCache) SPopN(key string, max int64) []string {
	start := getNow(r.engine.hasRedisLogger)
	val, err := r.client.SPopN(context.Background(), key, max).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("SPOPN %s %d", key, max)
		r.fillLogFields("SPOPN", message, start, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Del(keys ...string) {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.Del(context.Background(), keys...).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("DEL", "DEL "+strings.Join(keys, " "), start, err)
	}
	checkError(err)
}

func (r *RedisCache) XTrim(stream string, maxLen int64) (deleted int64) {
	start := getNow(r.engine.hasRedisLogger)
	var err error
	deleted, err = r.client.XTrimMaxLen(context.Background(), stream, maxLen).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XTREAM %s %d", stream, maxLen)
		r.fillLogFields("XTREAM", message, start, err)
	}
	checkError(err)
	return deleted
}

func (r *RedisCache) XRange(stream, start, stop string, count int64) []redis.XMessage {
	s := getNow(r.engine.hasRedisLogger)
	deleted, err := r.client.XRangeN(context.Background(), stream, start, stop, count).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields("XTREAM", message, s, err)
	}
	checkError(err)
	return deleted
}

func (r *RedisCache) XRevRange(stream, start, stop string, count int64) []redis.XMessage {
	s := getNow(r.engine.hasRedisLogger)
	deleted, err := r.client.XRevRangeN(context.Background(), stream, start, stop, count).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XREVRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields("XREVRANGE", message, s, err)
	}
	checkError(err)
	return deleted
}

func (r *RedisCache) XInfoStream(stream string) *redis.XInfoStream {
	start := getNow(r.engine.hasRedisLogger)
	info, err := r.client.XInfoStream(context.Background(), stream).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("XINFOSTREAM", "XINFOSTREAM "+stream, start, err)
	}
	checkError(err)
	return info
}

func (r *RedisCache) XInfoGroups(stream string) []redis.XInfoGroup {
	start := getNow(r.engine.hasRedisLogger)
	info, err := r.client.XInfoGroups(context.Background(), stream).Result()
	if err != nil && err.Error() == "ERR no such key" {
		if r.engine.hasRedisLogger {
			r.fillLogFields("XINFOGROUPS", "XINFOGROUPS "+stream, start, err)
		}
		return make([]redis.XInfoGroup, 0)
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("XINFOGROUPS", "XINFOGROUPS "+stream, start, err)
	}
	checkError(err)
	return info
}

func (r *RedisCache) XGroupCreate(stream, group, start string) (key string, exists bool) {
	s := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XGroupCreate(context.Background(), stream, group, start).Result()
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		if r.engine.hasRedisLogger {
			message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
			r.fillLogFields("XGROUPCREATE", message, s, err)
		}
		return "OK", true
	}
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
		r.fillLogFields("XGROUPCREATE", message, s, err)
	}
	checkError(err)
	return res, false
}

func (r *RedisCache) XGroupCreateMkStream(stream, group, start string) (key string, exists bool) {
	s := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XGroupCreateMkStream(context.Background(), stream, group, start).Result()
	created := false
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XGROUPCRMKSM %s %s %s", stream, group, start)
		r.fillLogFields("XGROUPCREATEMKSTREAM", message, s, err)
	}
	checkError(err)
	return res, created
}

func (r *RedisCache) XGroupDestroy(stream, group string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XGroupDestroy(context.Background(), stream, group).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XGROUPCDESTROY %s %s", stream, group)
		r.fillLogFields("XGROUPCDESTROY", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) XRead(a *redis.XReadArgs) []redis.XStream {
	start := getNow(r.engine.hasRedisLogger)
	info, err := r.client.XRead(context.Background(), a).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XREAD %s COUNT %d BLOCK %d", strings.Join(a.Streams, " "), a.Count, a.Block)
		r.fillLogFields("XREAD", message, start, err)
	}
	checkError(err)
	return info
}

func (r *RedisCache) XDel(stream string, ids ...string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	deleted, err := r.client.XDel(context.Background(), stream, ids...).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("XDEL", "XDEL "+strings.Join(ids, " "), start, err)
	}
	checkError(err)
	return deleted
}

func (r *RedisCache) XGroupDelConsumer(stream, group, consumer string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	deleted, err := r.client.XGroupDelConsumer(context.Background(), stream, group, consumer).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XGROUPDELCONSUMER %s %s %s", stream, group, consumer)
		r.fillLogFields("XGROUPDELCONSUMER", message, start, err)
	}
	checkError(err)
	return deleted
}

func (r *RedisCache) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) (streams []redis.XStream) {
	start := getNow(r.engine.hasRedisLogger)
	if r.engine.hasRedisLogger && a.Block >= 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d BLOCK %s NOACK %v", a.Count, a.Block.String(), a.NoAck)
		r.fillLogFields("XREADGROUP", message, start, nil)
	}
	streams, err := r.client.XReadGroup(ctx, a).Result()
	if err == redis.Nil {
		err = nil
	}
	if r.engine.hasRedisLogger && a.Block < 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d BLOCK %s NOACK %v", a.Count, a.Block.String(), a.NoAck)
		r.fillLogFields("XREADGROUP", message, start, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	checkError(err)
	return streams
}

func (r *RedisCache) XPending(stream, group string) *redis.XPending {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XPending(context.Background(), stream, group).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XPENDING %s %s", stream, group)
		r.fillLogFields("XPENDING", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) XPendingExt(a *redis.XPendingExtArgs) []redis.XPendingExt {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XPendingExt(context.Background(), a).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XPENDINGEXT %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" START %s END %s COUNT %d IDLE %s", a.Start, a.End, a.Count, a.Idle.String())
		r.fillLogFields("XPENDINGEXT", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) xAdd(stream string, values interface{}) (id string) {
	a := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	start := getNow(r.engine.hasRedisLogger)
	id, err := r.client.XAdd(context.Background(), a).Result()
	if r.engine.hasRedisLogger {
		message := "XADD " + stream + " " + strings.Join(values.([]string), " ")
		r.fillLogFields("XADD", message, start, err)
	}
	checkError(err)
	return id
}

func (r *RedisCache) XLen(stream string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	l, err := r.client.XLen(context.Background(), stream).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("XLEN", "XLEN "+stream, start, err)
	}
	checkError(err)
	return l
}

func (r *RedisCache) XClaim(a *redis.XClaimArgs) []redis.XMessage {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XClaim(context.Background(), a).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XCLAIM %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields("XCLAIM", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) XClaimJustID(a *redis.XClaimArgs) []string {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XClaimJustID(context.Background(), a).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XCLAIMJUSTID %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields("XCLAIMJUSTID", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) XAck(stream, group string, ids ...string) int64 {
	start := getNow(r.engine.hasRedisLogger)
	res, err := r.client.XAck(context.Background(), stream, group, ids...).Result()
	if r.engine.hasRedisLogger {
		message := fmt.Sprintf("XACK %s %s %s", stream, group, strings.Join(ids, " "))
		r.fillLogFields("XACK", message, start, err)
	}
	checkError(err)
	return res
}

func (r *RedisCache) FlushAll() {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.FlushAll(context.Background()).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("FLUSHALL", "FLUSHALL", start, err)
	}
	checkError(err)
}

func (r *RedisCache) FlushDB() {
	start := getNow(r.engine.hasRedisLogger)
	_, err := r.client.FlushDB(context.Background()).Result()
	if r.engine.hasRedisLogger {
		r.fillLogFields("FLUSHDB", "FLUSHDB", start, err)
	}
	checkError(err)
}

func (r *RedisCache) fillLogFields(operation, query string, start *time.Time, err error) {
	fillLogFields(r.engine.queryLoggersRedis, r.config.GetCode(), sourceRedis, operation, query, start, err)
}
