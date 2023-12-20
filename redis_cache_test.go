package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6385", 15, DefaultPoolCode, nil)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	orm := validatedRegistry.NewORM(context.Background())

	r := orm.Engine().Redis(DefaultPoolCode)

	testLogger := &MockLogHandler{}
	orm.RegisterQueryLogger(testLogger, false, true, false)
	r.FlushDB(orm)
	testLogger.Clear()

	valid := false
	val := r.GetSet(orm, "test_get_set", time.Second*10, func() any {
		valid = true
		return "ok"
	})
	assert.True(t, valid)
	assert.Equal(t, "ok", val)
	valid = false
	val = r.GetSet(orm, "test_get_set", time.Second*10, func() any {
		valid = true
		return "ok"
	})
	assert.False(t, valid)
	assert.Equal(t, "ok", val)

	val, has := r.Get(orm, "test_get")
	assert.False(t, has)
	assert.Equal(t, "", val)
	r.Set(orm, "test_get", "hello", 1*time.Second)
	val, has = r.Get(orm, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
	isSet := r.SetNX(orm, "test_get_nx", "hello nx", 1*time.Second)
	assert.True(t, isSet)
	val, has = r.Get(orm, "test_get_nx")
	assert.True(t, has)
	assert.Equal(t, "hello nx", val)
	isSet = r.SetNX(orm, "test_get_nx", "hello nx", 1*time.Second)
	assert.False(t, isSet)

	r.LPush(orm, "test_list", "a")
	assert.Equal(t, int64(1), r.LLen(orm, "test_list"))
	r.RPush(orm, "test_list", "b", "c")
	assert.Equal(t, int64(3), r.LLen(orm, "test_list"))
	assert.Equal(t, []string{"a", "b", "c"}, r.LRange(orm, "test_list", 0, 2))
	assert.Equal(t, []string{"b", "c"}, r.LRange(orm, "test_list", 1, 5))
	r.LSet(orm, "test_list", 1, "d")
	assert.Equal(t, []string{"a", "d", "c"}, r.LRange(orm, "test_list", 0, 2))
	r.LRem(orm, "test_list", 1, "c")
	assert.Equal(t, []string{"a", "d"}, r.LRange(orm, "test_list", 0, 2))

	val, has = r.RPop(orm, "test_list")
	assert.True(t, has)
	assert.Equal(t, "d", val)
	r.Ltrim(orm, "test_list", 1, 2)
	val, has = r.RPop(orm, "test_list")
	assert.False(t, has)
	assert.Equal(t, "", val)

	r.HSet(orm, "test_map", "name", "Tom")
	assert.Equal(t, map[string]string{"name": "Tom"}, r.HGetAll(orm, "test_map"))
	v, has := r.HGet(orm, "test_map", "name")
	assert.True(t, has)
	assert.Equal(t, "Tom", v)
	_, has = r.HGet(orm, "test_map", "name2")
	assert.False(t, has)

	r.HSet(orm, "test_map", "last", "Summer", "age", "16")
	assert.Equal(t, map[string]string{"age": "16", "last": "Summer", "name": "Tom"}, r.HGetAll(orm, "test_map"))
	assert.Equal(t, map[string]any{"age": "16", "missing": nil, "name": "Tom"}, r.HMGet(orm, "test_map",
		"name", "age", "missing"))

	r.HDel(orm, "test_map", "age")
	assert.Equal(t, map[string]string{"last": "Summer", "name": "Tom"}, r.HGetAll(orm, "test_map"))
	assert.Equal(t, int64(2), r.HLen(orm, "test_map"))

	assert.True(t, r.HSetNx(orm, "test_map_nx", "key", "value"))
	assert.False(t, r.HSetNx(orm, "test_map_nx", "key", "value"))

	val = r.HIncrBy(orm, "test_inc", "a", 2)
	assert.Equal(t, int64(2), val)
	val = r.HIncrBy(orm, "test_inc", "a", 3)
	assert.Equal(t, int64(5), val)

	val = r.IncrBy(orm, "test_inc_2", 2)
	assert.Equal(t, int64(2), val)
	val = r.Incr(orm, "test_inc_2")
	assert.Equal(t, int64(3), val)

	val = r.IncrWithExpire(orm, "test_inc_exp", time.Second)
	assert.Equal(t, int64(1), val)
	val = r.IncrWithExpire(orm, "test_inc_exp", time.Second)
	assert.Equal(t, int64(2), val)
	time.Sleep(time.Millisecond * 1200)
	val = r.IncrWithExpire(orm, "test_inc_exp", time.Second)
	assert.Equal(t, int64(1), val)

	assert.True(t, r.Expire(orm, "test_map", time.Second*1))
	assert.Equal(t, int64(1), r.Exists(orm, "test_map"))
	time.Sleep(time.Millisecond * 1200)
	assert.Equal(t, int64(0), r.Exists(orm, "test_map"))

	added := r.ZAdd(orm, "test_z", redis.Z{Member: "a", Score: 10}, redis.Z{Member: "b", Score: 20})
	assert.Equal(t, int64(2), added)
	assert.Equal(t, []string{"b", "a"}, r.ZRevRange(orm, "test_z", 0, 3))
	assert.Equal(t, float64(10), r.ZScore(orm, "test_z", "a"))
	resZRange := r.ZRangeWithScores(orm, "test_z", 0, 3)
	assert.Len(t, resZRange, 2)
	assert.Equal(t, "a", resZRange[0].Member)
	assert.Equal(t, "b", resZRange[1].Member)
	assert.Equal(t, float64(10), resZRange[0].Score)
	assert.Equal(t, float64(20), resZRange[1].Score)
	resZRange = r.ZRevRangeWithScores(orm, "test_z", 0, 3)
	assert.Len(t, resZRange, 2)
	assert.Equal(t, "b", resZRange[0].Member)
	assert.Equal(t, "a", resZRange[1].Member)
	assert.Equal(t, float64(20), resZRange[0].Score)
	assert.Equal(t, float64(10), resZRange[1].Score)

	assert.Equal(t, int64(2), r.ZCard(orm, "test_z"))
	assert.Equal(t, int64(2), r.ZCount(orm, "test_z", "10", "20"))
	assert.Equal(t, int64(1), r.ZCount(orm, "test_z", "11", "20"))
	r.Del(orm, "test_z")
	assert.Equal(t, int64(0), r.ZCount(orm, "test_z", "10", "20"))

	r.MSet(orm, "key_1", "a", "key_2", "b")
	assert.Equal(t, []any{"a", "b", nil}, r.MGet(orm, "key_1", "key_2", "missing"))

	added = r.SAdd(orm, "test_s", "a", "b", "c", "d", "a")
	assert.Equal(t, int64(4), added)
	assert.Equal(t, int64(4), r.SCard(orm, "test_s"))
	val, has = r.SPop(orm, "test_s")
	assert.NotEqual(t, "", val)
	assert.True(t, has)
	assert.Len(t, r.SPopN(orm, "test_s", 10), 3)
	assert.Len(t, r.SPopN(orm, "test_s", 10), 0)
	val, has = r.SPop(orm, "test_s")
	assert.Equal(t, "", val)
	assert.False(t, has)

	script := `
		local count = 2	
		return count + KEYS[1] + ARGV[1]
	`
	val = r.Eval(orm, script, []string{"3"}, 7)
	assert.Equal(t, int64(12), val)
	val = r.ScriptLoad(orm, script)

	assert.False(t, r.ScriptExists(orm, "invalid"))
	assert.True(t, r.ScriptExists(orm, "618358a5df682faed583025e34f07905c2a96823"))

	assert.Equal(t, "618358a5df682faed583025e34f07905c2a96823", val)
	val, exists := r.EvalSha(orm, val.(string), []string{"3"}, 8)
	assert.Equal(t, int64(13), val)
	assert.True(t, exists)

	val, exists = r.EvalSha(orm, "invalid", []string{"3"}, 8)
	assert.Nil(t, val)
	assert.False(t, exists)

	r.Set(orm, "a", "n", 10*time.Second)
	r.FlushAll(orm)
	assert.Equal(t, int64(0), r.Exists(orm, "a"))

	res := r.Info(orm)
	assert.Contains(t, res, "redis_version")

	r.LPush(orm, "test_list", "test")
	has, _ = orm.getRedisLoggers()
	assert.Equal(t, "list", r.Type(orm, "test_list"))

	val = r.LMove(orm, "test_list", "test_list_next", "RIGHT", "LEFT")
	assert.Equal(t, "test", val)
	val = r.BLMove(orm, "test_list_next", "test_list", "RIGHT", "LEFT", time.Second)
	assert.Equal(t, "test", val)
	r.SAdd(orm, "test_set", "test", "value")
	assert.True(t, r.SIsMember(orm, "test_set", "test"))

	registry = NewRegistry()
	registry.RegisterRedis("localhost:6399", 15, DefaultPoolCode, nil)
	validatedRegistry, err = registry.Validate()
	assert.NoError(t, err)
	orm = validatedRegistry.NewORM(context.Background())
	testLogger = &MockLogHandler{}
	orm.RegisterQueryLogger(testLogger, false, true, false)
	assert.Panics(t, func() {
		orm.Engine().Redis(DefaultPoolCode).Get(orm, "invalid")
	})

	registry = NewRegistry()
	registry.RegisterRedis("localhost:6385", 15, DefaultPoolCode, &RedisOptions{User: "user", Password: "pass"})
	validatedRegistry, err = registry.Validate()
	assert.Nil(t, err)
	orm = validatedRegistry.NewORM(context.Background())
	assert.PanicsWithError(t, "WRONGPASS invalid username-password pair or user is disabled.", func() {
		orm.Engine().Redis(DefaultPoolCode).Incr(orm, "test")
	})
}
