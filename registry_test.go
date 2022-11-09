package beeorm

import (
	"testing"

	"github.com/go-redis/redis/v8"

	"github.com/stretchr/testify/assert"
)

func TestRegisterRedisSentinelWithOptions(t *testing.T) {
	registry := &Registry{}
	opt := redis.FailoverOptions{}
	opt.Username = "test_user"
	opt.Password = "test_pass"
	sentinels := []string{"127.0.0.1:23", "127.0.0.1:24"}

	registry.RegisterRedisSentinelWithOptions("my_namespoace", opt, 0, sentinels)
	vRegistry, f, err := registry.Validate()
	assert.NoError(t, err)
	assert.NotNil(t, f)
	pools := vRegistry.GetRedisPools()
	assert.Len(t, pools, 1)
	engine := vRegistry.CreateEngine()
	outputOptions := engine.GetRedis().client.Options()
	assert.Equal(t, "test_user", outputOptions.Username)
	assert.Equal(t, "test_pass", outputOptions.Password)
}
