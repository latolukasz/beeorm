package simple_metrics

import (
	"testing"

	"github.com/latolukasz/beeorm/v2"
	"github.com/stretchr/testify/assert"
)

type simpleMetricsEntity struct {
	beeorm.ORM
	Name string `orm:"required"`
}

func TestMysqlMetrics(t *testing.T) {
	registry := &beeorm.Registry{}
	var entity *simpleMetricsEntity
	plugin := Init(&Options{MySQL: true})
	registry.RegisterPlugin(plugin)
	engine := beeorm.PrepareTables(t, registry, 8, 6, "", entity)

	plugin = engine.GetRegistry().GetPlugin(PluginCode).(*Plugin)
	dbStats := plugin.GetMySQLStats()
	assert.NotEmpty(t, dbStats)
	assert.NotNil(t, dbStats["default"])
	plugin.ClearStats()
	dbStats = plugin.GetMySQLStats()
	assert.Len(t, dbStats["default"], 0)

	entity = &simpleMetricsEntity{Name: "One"}
	engine.Flush(entity)
	entity = &simpleMetricsEntity{Name: "Two"}
	engine.Flush(entity)
	dbStats = plugin.GetMySQLStats()
	assert.Len(t, dbStats["default"], 1)
	assert.Len(t, dbStats["default"][Insert], 1)
	assert.Len(t, dbStats["default"][Insert]["simplemetricsentity"], 1)
	assert.NotNil(t, dbStats["default"][Insert]["simplemetricsentity"][false])
	assert.Equal(t, uint64(2), dbStats["default"][Insert]["simplemetricsentity"][false].counter)

	entity = &simpleMetricsEntity{Name: "Three"}
	engine.FlushLazy(entity)
	beeorm.RunLazyFlushConsumer(engine, false)
	assert.Len(t, dbStats["default"], 1)
	assert.Equal(t, uint64(2), dbStats["default"][Insert]["simplemetricsentity"][false].counter)
	assert.Equal(t, uint64(1), dbStats["default"][Insert]["simplemetricsentity"][true].counter)

	plugin.ClearStats()

	entity = &simpleMetricsEntity{}
	engine.LoadByID(1, entity)
	entity.Name = "OneV2"
	engine.Flush(entity)
	dbStats = plugin.GetMySQLStats()
	assert.Equal(t, uint64(1), dbStats["default"][Update]["simplemetricsentity"][false].counter)

	plugin.ClearStats()

	engine.Delete(entity)
	dbStats = plugin.GetMySQLStats()
	assert.Equal(t, uint64(1), dbStats["default"][Delete]["simplemetricsentity"][false].counter)

	plugin.ClearStats()
	engine.LoadByID(2, entity)
}
