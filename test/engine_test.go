package test

import (
	"testing"

	"github.com/latolukasz/beeorm/v2"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {
	engine := PrepareTables(t, &beeorm.Registry{}, 5, 6, "")
	source := engine.GetRegistry().GetSourceRegistry()
	assert.NotNil(t, source)
	assert.PanicsWithError(t, "unregistered mysql pool 'test'", func() {
		engine.GetMysql("test")
	})
	assert.PanicsWithError(t, "unregistered local cache pool 'test'", func() {
		engine.GetLocalCache("test")
	})
	assert.PanicsWithError(t, "unregistered redis cache pool 'test'", func() {
		engine.GetRedis("test")
	})
}
