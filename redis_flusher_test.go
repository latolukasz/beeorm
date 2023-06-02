package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisFlusher(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", "", 15)
	registry.RegisterRedis("localhost:6382", "", 15, "second")
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	registry.RegisterRedisStream("test-stream-2", "default", []string{"test-group-2"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	testLogger := &testLogHandler{}
	engine.RegisterQueryLogger(testLogger, false, true, false)

	flusher := &redisFlusher{engine: engine.(*engineImplementation)}

	flusher.Del("default")
	flusher.Del("default", "del_key")
	flusher.Del("default", "del_key_2")
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "default", testLogger.Logs[0]["pool"])
	assert.Equal(t, "DEL", testLogger.Logs[0]["operation"])
	assert.Equal(t, "DEL del_key del_key_2", testLogger.Logs[0]["query"])

	testLogger.clear()
	flusher.Publish("test-stream", "my_body")
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "default", testLogger.Logs[0]["pool"])
	assert.Equal(t, "XADD", testLogger.Logs[0]["operation"])

	testLogger.clear()
	flusher.Publish("test-stream", "my_body")
	flusher.Publish("test-stream", "my_body_2")
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "default", testLogger.Logs[0]["pool"])
	assert.Equal(t, "PIPELINE EXEC", testLogger.Logs[0]["operation"])

	testLogger.clear()
	flusher.HSet("default", "my_key", "a", "b", "c", "d")
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "default", testLogger.Logs[0]["pool"])
	assert.Equal(t, "HSET", testLogger.Logs[0]["operation"])
	assert.Equal(t, "HSET my_key  a b c d", testLogger.Logs[0]["query"])

	testLogger.clear()
	flusher.Del("default", "del_key")
	flusher.Del("default", "del_key_2")
	flusher.Publish("test-stream", "my_body")
	flusher.Publish("test-stream-2", "my_body")
	flusher.HSet("default", "my_key", "a", "b")
	flusher.HSet("default", "my_key", "c", "d")
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "default", testLogger.Logs[0]["pool"])
	assert.Equal(t, "PIPELINE EXEC", testLogger.Logs[0]["operation"])

	testLogger.clear()
	flusher.Del("default", "my_key")
	flusher.Del("second", "my_key_2")
	flusher.Flush()
	assert.Len(t, testLogger.Logs, 2)
	if testLogger.Logs[0]["pool"] == "default" {
		assert.Equal(t, "default", testLogger.Logs[0]["pool"])
		assert.Equal(t, "DEL", testLogger.Logs[0]["operation"])
		assert.Equal(t, "DEL my_key", testLogger.Logs[0]["query"])
		assert.Equal(t, "second", testLogger.Logs[1]["pool"])
		assert.Equal(t, "DEL", testLogger.Logs[1]["operation"])
		assert.Equal(t, "DEL my_key_2", testLogger.Logs[1]["query"])
	} else {
		assert.Equal(t, "default", testLogger.Logs[1]["pool"])
		assert.Equal(t, "DEL", testLogger.Logs[1]["operation"])
		assert.Equal(t, "DEL my_key", testLogger.Logs[1]["query"])
		assert.Equal(t, "second", testLogger.Logs[0]["pool"])
		assert.Equal(t, "DEL", testLogger.Logs[0]["operation"])
		assert.Equal(t, "DEL my_key_2", testLogger.Logs[0]["query"])
	}
}
