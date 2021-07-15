package tools

import (
	"context"
	"testing"

	jsoniter "github.com/json-iterator/go"

	"github.com/latolukasz/beeorm"
	"github.com/stretchr/testify/assert"
)

func TestRedisSearchStatistics(t *testing.T) {
	registry := &beeorm.Registry{}
	registry.RegisterRedis("localhost:6382", 0)
	registry.RegisterRedisSearchIndex(&beeorm.RedisSearchIndex{Name: "test", RedisPool: "default"})
	ctx := context.Background()
	validatedRegistry, err := registry.Validate(ctx)
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	for _, alter := range engine.GetRedisSearchIndexAlters() {
		alter.Execute()
	}
	stats := GetRedisSearchStatistics(engine)
	assert.Len(t, stats, 1)
	assert.Equal(t, "test", stats[0].Index.Name)
	assert.Len(t, stats[0].Versions, 1)
	asJSON, err := jsoniter.ConfigFastest.Marshal(stats)
	assert.NoError(t, err)
	assert.NotEmpty(t, asJSON)
}
