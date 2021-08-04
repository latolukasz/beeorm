package tools

import (
	"context"
	"testing"

	"github.com/latolukasz/beeorm"
	"github.com/stretchr/testify/assert"
)

func TestRedisStatistics(t *testing.T) {
	registry := &beeorm.Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedis("localhost:6382", 14, "another")
	ctx := context.Background()
	validatedRegistry, def, err := registry.Validate(ctx)
	assert.NoError(t, err)
	defer def()
	engine := validatedRegistry.CreateEngine(ctx)
	r := engine.GetRedis()
	r.FlushDB()

	stats := GetRedisStatistics(engine)
	assert.Len(t, stats, 1)
}
