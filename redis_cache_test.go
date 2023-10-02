package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
)

func TestRedis6(t *testing.T) {
	testRedis(t, 6)
}

func TestRedis7(t *testing.T) {
	testRedis(t, 7)
}

func testRedis(t *testing.T, version int) {
	registry := &Registry{}
	url := "localhost:6382"
	if version == 7 {
		url = "localhost:6381"
	}
	registry.RegisterRedis(url, 15)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	c := validatedRegistry.NewContext(context.Background())

	r := c.Engine().Redis(DefaultPoolCode)

	testLogger := &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, true, false)
	r.FlushDB(c)
	testLogger.Clear()

	valid := false
	val := r.GetSet(c, "test_get_set", time.Second*10, func() interface{} {
		valid = true
		return "ok"
	})
	assert.True(t, valid)
	assert.Equal(t, "ok", val)
	valid = false
	val = r.GetSet(c, "test_get_set", time.Second*10, func() interface{} {
		valid = true
		return "ok"
	})
	assert.False(t, valid)
	assert.Equal(t, "ok", val)

	val, has := r.Get(c, "test_get")
	assert.False(t, has)
	assert.Equal(t, "", val)
	r.Set(c, "test_get", "hello", 1*time.Second)
	val, has = r.Get(c, "test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)
	isSet := r.SetNX(c, "test_get_nx", "hello nx", 1*time.Second)
	assert.True(t, isSet)
	val, has = r.Get(c, "test_get_nx")
	assert.True(t, has)
	assert.Equal(t, "hello nx", val)
	isSet = r.SetNX(c, "test_get_nx", "hello nx", 1*time.Second)
	assert.False(t, isSet)

	r.LPush(c, "test_list", "a")
	assert.Equal(t, int64(1), r.LLen(c, "test_list"))
	r.RPush(c, "test_list", "b", "c")
	assert.Equal(t, int64(3), r.LLen(c, "test_list"))
	assert.Equal(t, []string{"a", "b", "c"}, r.LRange(c, "test_list", 0, 2))
	assert.Equal(t, []string{"b", "c"}, r.LRange(c, "test_list", 1, 5))
	r.LSet(c, "test_list", 1, "d")
	assert.Equal(t, []string{"a", "d", "c"}, r.LRange(c, "test_list", 0, 2))
	r.LRem(c, "test_list", 1, "c")
	assert.Equal(t, []string{"a", "d"}, r.LRange(c, "test_list", 0, 2))

	val, has = r.RPop(c, "test_list")
	assert.True(t, has)
	assert.Equal(t, "d", val)
	r.Ltrim(c, "test_list", 1, 2)
	val, has = r.RPop(c, "test_list")
	assert.False(t, has)
	assert.Equal(t, "", val)

	r.HSet(c, "test_map", "name", "Tom")
	assert.Equal(t, map[string]string{"name": "Tom"}, r.HGetAll(c, "test_map"))
	v, has := r.HGet(c, "test_map", "name")
	assert.True(t, has)
	assert.Equal(t, "Tom", v)
	_, has = r.HGet(c, "test_map", "name2")
	assert.False(t, has)

	r.HSet(c, "test_map", "last", "Summer", "age", "16")
	assert.Equal(t, map[string]string{"age": "16", "last": "Summer", "name": "Tom"}, r.HGetAll(c, "test_map"))
	assert.Equal(t, map[string]interface{}{"age": "16", "missing": nil, "name": "Tom"}, r.HMGet(c, "test_map",
		"name", "age", "missing"))

	r.HDel(c, "test_map", "age")
	assert.Equal(t, map[string]string{"last": "Summer", "name": "Tom"}, r.HGetAll(c, "test_map"))
	assert.Equal(t, int64(2), r.HLen(c, "test_map"))

	assert.True(t, r.HSetNx(c, "test_map_nx", "key", "value"))
	assert.False(t, r.HSetNx(c, "test_map_nx", "key", "value"))

	val = r.HIncrBy(c, "test_inc", "a", 2)
	assert.Equal(t, int64(2), val)
	val = r.HIncrBy(c, "test_inc", "a", 3)
	assert.Equal(t, int64(5), val)

	val = r.IncrBy(c, "test_inc_2", 2)
	assert.Equal(t, int64(2), val)
	val = r.Incr(c, "test_inc_2")
	assert.Equal(t, int64(3), val)

	val = r.IncrWithExpire(c, "test_inc_exp", time.Second)
	assert.Equal(t, int64(1), val)
	val = r.IncrWithExpire(c, "test_inc_exp", time.Second)
	assert.Equal(t, int64(2), val)
	time.Sleep(time.Millisecond * 1200)
	val = r.IncrWithExpire(c, "test_inc_exp", time.Second)
	assert.Equal(t, int64(1), val)

	assert.True(t, r.Expire(c, "test_map", time.Second*1))
	assert.Equal(t, int64(1), r.Exists(c, "test_map"))
	time.Sleep(time.Millisecond * 1200)
	assert.Equal(t, int64(0), r.Exists(c, "test_map"))

	added := r.ZAdd(c, "test_z", redis.Z{Member: "a", Score: 10}, redis.Z{Member: "b", Score: 20})
	assert.Equal(t, int64(2), added)
	assert.Equal(t, []string{"b", "a"}, r.ZRevRange(c, "test_z", 0, 3))
	assert.Equal(t, float64(10), r.ZScore(c, "test_z", "a"))
	resZRange := r.ZRangeWithScores(c, "test_z", 0, 3)
	assert.Len(t, resZRange, 2)
	assert.Equal(t, "a", resZRange[0].Member)
	assert.Equal(t, "b", resZRange[1].Member)
	assert.Equal(t, float64(10), resZRange[0].Score)
	assert.Equal(t, float64(20), resZRange[1].Score)
	resZRange = r.ZRevRangeWithScores(c, "test_z", 0, 3)
	assert.Len(t, resZRange, 2)
	assert.Equal(t, "b", resZRange[0].Member)
	assert.Equal(t, "a", resZRange[1].Member)
	assert.Equal(t, float64(20), resZRange[0].Score)
	assert.Equal(t, float64(10), resZRange[1].Score)

	assert.Equal(t, int64(2), r.ZCard(c, "test_z"))
	assert.Equal(t, int64(2), r.ZCount(c, "test_z", "10", "20"))
	assert.Equal(t, int64(1), r.ZCount(c, "test_z", "11", "20"))
	r.Del(c, "test_z")
	assert.Equal(t, int64(0), r.ZCount(c, "test_z", "10", "20"))

	r.MSet(c, "key_1", "a", "key_2", "b")
	assert.Equal(t, []interface{}{"a", "b", nil}, r.MGet(c, "key_1", "key_2", "missing"))

	added = r.SAdd(c, "test_s", "a", "b", "c", "d", "a")
	assert.Equal(t, int64(4), added)
	assert.Equal(t, int64(4), r.SCard(c, "test_s"))
	val, has = r.SPop(c, "test_s")
	assert.NotEqual(t, "", val)
	assert.True(t, has)
	assert.Len(t, r.SPopN(c, "test_s", 10), 3)
	assert.Len(t, r.SPopN(c, "test_s", 10), 0)
	val, has = r.SPop(c, "test_s")
	assert.Equal(t, "", val)
	assert.False(t, has)

	assert.NotEmpty(t, r.Info(c, "modules"))

	script := `
		local count = 2	
		return count + KEYS[1] + ARGV[1]
	`
	val = r.Eval(c, script, []string{"3"}, 7)
	assert.Equal(t, int64(12), val)
	val = r.ScriptLoad(c, script)

	assert.False(t, r.ScriptExists(c, "invalid"))
	assert.True(t, r.ScriptExists(c, "618358a5df682faed583025e34f07905c2a96823"))

	assert.Equal(t, "618358a5df682faed583025e34f07905c2a96823", val)
	val, exists := r.EvalSha(c, val.(string), []string{"3"}, 8)
	assert.Equal(t, int64(13), val)
	assert.True(t, exists)

	val, exists = r.EvalSha(c, "invalid", []string{"3"}, 8)
	assert.Nil(t, val)
	assert.False(t, exists)

	r.Set(c, "a", "n", 10*time.Second)
	r.FlushAll(c)
	assert.Equal(t, int64(0), r.Exists(c, "a"))

	registry = &Registry{}
	registry.RegisterRedis("localhost:6399", 15)
	validatedRegistry, err = registry.Validate()
	assert.NoError(t, err)
	c = validatedRegistry.NewContext(context.Background())
	testLogger = &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, true, false)
	assert.Panics(t, func() {
		c.Engine().Redis(DefaultPoolCode).Get(c, "invalid")
	})

	registry = &Registry{}
	registry.RegisterRedisWithCredentials("localhost:6382", "user", "pass", 15)
	validatedRegistry, err = registry.Validate()
	assert.Nil(t, err)
	c = validatedRegistry.NewContext(context.Background())
	assert.PanicsWithError(t, "WRONGPASS invalid username-password pair or user is disabled.", func() {
		c.Engine().Redis(DefaultPoolCode).Incr(c, "test")
	})
}
