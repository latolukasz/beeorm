package beeorm

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
)

func TestRedis6(t *testing.T) {
	testRedis(t, "", 6)
}

func TestRedis7(t *testing.T) {
	testRedis(t, "", 7)
}

func TestRedis6Namespaces(t *testing.T) {
	testRedis(t, "test", 6)
}

func TestRedis7Namespaces(t *testing.T) {
	testRedis(t, "test", 7)
}

func testRedis(t *testing.T, namespace string, version int) {
	registry := &Registry{}
	url := "localhost:6382"
	if version == 7 {
		url = "localhost:6381"
	}
	registry.RegisterRedis(url, namespace, 15)
	registry.RegisterRedisStream("test-stream", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream", "test-group")
	registry.RegisterRedisStream("test-stream-a", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream-a", "test-group")
	registry.RegisterRedisStream("test-stream-b", "default")
	registry.RegisterRedisStreamConsumerGroups("test-stream-b", "test-group")

	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	c := validatedRegistry.NewContext(context.Background())

	r := c.Engine().Redis()

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

	id := c.EventBroker().Publish("test-stream", "a", nil)
	assert.NotEmpty(t, id)
	assert.Equal(t, int64(1), r.XLen(c, "test-stream"))
	assert.Equal(t, int64(1), r.XTrim(c, "test-stream", 0))
	assert.Equal(t, int64(0), r.XLen(c, "test-stream"))

	c.EventBroker().Publish("test-stream", "a1", nil)
	c.EventBroker().Publish("test-stream", "a2", nil)
	c.EventBroker().Publish("test-stream", "a3", nil)
	c.EventBroker().Publish("test-stream", "a4", nil)
	c.EventBroker().Publish("test-stream", "a5", nil)
	res, has := r.XGroupCreate(c, "test-stream", "test-group", "0")
	assert.Equal(t, "OK", res)
	assert.False(t, has)
	assert.Equal(t, int64(1), r.Exists(c, "test-stream"))
	assert.Equal(t, "stream", r.Type(c, "test-stream"))
	res, has = r.XGroupCreate(c, "test-stream", "test-group", "0")
	assert.True(t, has)
	assert.Equal(t, "OK", res)
	res, has = r.XGroupCreateMkStream(c, "test-stream-2", "test-group-2", "$")
	assert.False(t, has)
	assert.Equal(t, "OK", res)
	assert.Equal(t, int64(1), r.Exists(c, "test-stream-2"))
	res, has = r.XGroupCreateMkStream(c, "test-stream-2", "test-group-2", "$")
	assert.True(t, has)
	assert.Equal(t, "OK", res)
	assert.Equal(t, int64(1), r.Exists(c, "test-stream-2"))
	deleted := r.XGroupDestroy(c, "test-stream-2", "test-group-2")
	assert.Equal(t, int64(1), deleted)
	assert.Equal(t, int64(0), r.Exists(c, "test-group-2"))
	info := r.XInfoStream(c, "test-stream")
	assert.Equal(t, int64(1), info.Groups)
	infoGroups := r.XInfoGroups(c, "test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, "test-group", infoGroups[0].Name)
	assert.Equal(t, "0-0", infoGroups[0].LastDeliveredID)
	assert.Equal(t, int64(0), infoGroups[0].Consumers)
	assert.Equal(t, int64(0), infoGroups[0].Pending)

	events := r.XRange(c, "test-stream", "-", "+", 2)
	assert.Len(t, events, 2)
	assert.Equal(t, "\xa2a1", events[0].Values["s"])
	assert.Equal(t, "\xa2a2", events[1].Values["s"])

	infoGroups = r.XInfoGroups(c, "test-stream-invalid")
	assert.Len(t, infoGroups, 0)

	events = r.XRevRange(c, "test-stream", "+", "-", 2)
	assert.Len(t, events, 2)
	assert.Equal(t, "\xa2a5", events[0].Values["s"])
	assert.Equal(t, "\xa2a4", events[1].Values["s"])

	tmpEventID := c.EventBroker().Publish("test-stream", "new", nil)
	assert.Equal(t, int64(1), r.XDel(c, "test-stream", tmpEventID))
	events = r.XRevRange(c, "test-stream", "+", "-", 2)
	assert.Len(t, events, 2)
	assert.Equal(t, "\xa2a5", events[0].Values["s"])
	assert.Equal(t, "\xa2a4", events[1].Values["s"])

	streams := r.XReadGroup(c, &redis.XReadGroupArgs{Group: "test-group", Streams: []string{"test-stream", ">"},
		Consumer: "test-consumer"})
	assert.Len(t, streams, 1)
	assert.Equal(t, "test-stream", streams[0].Stream)
	assert.Len(t, streams[0].Messages, 5)
	assert.Equal(t, "\xa2a1", streams[0].Messages[0].Values["s"])
	assert.Equal(t, int64(5), r.XLen(c, "test-stream"))
	infoGroups = r.XInfoGroups(c, "test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, int64(1), infoGroups[0].Consumers)
	assert.Equal(t, int64(5), infoGroups[0].Pending)
	streams2 := r.XReadGroup(c, &redis.XReadGroupArgs{Group: "test-group", Streams: []string{"test-stream", ">"},
		Consumer: "test-consumer", Block: -1})
	assert.Nil(t, streams2)
	streams2 = r.XReadGroup(c, &redis.XReadGroupArgs{Group: "test-group", Streams: []string{"test-stream", "0"},
		Consumer: "test-consumer", Block: -1})
	assert.Len(t, streams2, 1)
	assert.Len(t, streams2[0].Messages, 5)
	pending := r.XPending(c, "test-stream", "test-group")
	assert.Equal(t, int64(5), pending.Count)
	assert.Equal(t, int64(5), pending.Consumers["test-consumer"])
	pendingExt := r.XPendingExt(c, &redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-", End: "+"})
	assert.Len(t, pendingExt, 5)
	assert.Equal(t, "test-consumer", pendingExt[0].Consumer)
	assert.Equal(t, int64(2), pendingExt[0].RetryCount)
	time.Sleep(time.Millisecond * 2)
	messages := r.XClaim(c, &redis.XClaimArgs{Stream: "test-stream", Group: "test-group", Consumer: "test-consumer-2",
		MinIdle:  time.Millisecond,
		Messages: []string{pendingExt[0].ID, pendingExt[1].ID}})
	assert.Len(t, messages, 2)
	pendingExt = r.XPendingExt(c, &redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-",
		End: "+", Consumer: "test-consumer"})
	assert.Len(t, pendingExt, 3)
	testID := pendingExt[0].ID
	pendingExt = r.XPendingExt(c, &redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-",
		End: "+", Consumer: "test-consumer-2"})
	assert.Len(t, pendingExt, 2)
	infoGroups = r.XInfoGroups(c, "test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, int64(2), infoGroups[0].Consumers)
	assert.Equal(t, int64(5), infoGroups[0].Pending)
	confirmed := r.XAck(c, "test-stream", "test-group", pendingExt[0].ID, pendingExt[1].ID)
	assert.Equal(t, int64(2), confirmed)
	pendingExt = r.XPendingExt(c, &redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-",
		End: "+", Consumer: "test-consumer-2"})
	assert.Len(t, pendingExt, 0)
	infoGroups = r.XInfoGroups(c, "test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, int64(2), infoGroups[0].Consumers)
	assert.Equal(t, int64(3), infoGroups[0].Pending)
	ids := r.XClaimJustID(c, &redis.XClaimArgs{Stream: "test-stream", Group: "test-group", Consumer: "test-consumer-2",
		MinIdle: time.Millisecond, Messages: []string{testID, "2-2"}})
	assert.Len(t, ids, 1)
	assert.Equal(t, testID, ids[0])
	confirmed = r.XGroupDelConsumer(c, "test-stream", "test-group", "test-consumer-2")
	assert.Equal(t, int64(1), confirmed)
	infoGroups = r.XInfoGroups(c, "test-stream")
	assert.Equal(t, int64(1), infoGroups[0].Consumers)

	c.EventBroker().Publish("test-stream-a", "a1", nil)
	c.EventBroker().Publish("test-stream-b", "b1", nil)
	r.XGroupCreate(c, "test-stream-a", "test-group-ab", "0")
	r.XGroupCreate(c, "test-stream-b", "test-group-ab", "0")
	streams = r.XReadGroup(c, &redis.XReadGroupArgs{Group: "test-group-ab", Streams: []string{"test-stream-a", "test-stream-b", ">", ">"},
		Consumer: "test-consumer-ab", Block: -1})
	assert.Len(t, streams, 2)
	assert.Len(t, streams[0].Messages, 1)
	assert.Len(t, streams[1].Messages, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
	c = validatedRegistry.NewContext(ctx)
	defer cancel()
	now := time.Now()
	streams = r.XReadGroup(c, &redis.XReadGroupArgs{Group: "test-group-ab", Streams: []string{"test-stream-a", "test-stream-b", ">", ">"},
		Consumer: "test-consumer-ab", Block: time.Second * 3})
	assert.LessOrEqual(t, time.Since(now).Milliseconds(), int64(350))
	c = validatedRegistry.NewContext(context.Background())

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
	registry.RegisterRedis("localhost:6399", "", 15)
	validatedRegistry, err = registry.Validate()
	assert.NoError(t, err)
	c = validatedRegistry.NewContext(context.Background())
	testLogger = &MockLogHandler{}
	c.RegisterQueryLogger(testLogger, false, true, false)
	assert.Panics(t, func() {
		c.Engine().Redis().Get(c, "invalid")
	})

	registry = &Registry{}
	registry.RegisterRedisWithCredentials("localhost:6382", namespace, "user", "pass", 15)
	validatedRegistry, err = registry.Validate()
	assert.Nil(t, err)
	c = validatedRegistry.NewContext(context.Background())
	assert.PanicsWithError(t, "WRONGPASS invalid username-password pair or user is disabled.", func() {
		c.Engine().Redis().Incr(c, "test")
	})
}
