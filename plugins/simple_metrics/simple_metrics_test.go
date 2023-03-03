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

	plugin := Init(InitOptions().EnableMySQLMetrics().EnableMySQLSlowQuery(5))
	registry.RegisterPlugin(plugin)
	engine := beeorm.PrepareTables(t, registry, 8, 6, "", entity)

	plugin = engine.GetRegistry().GetPlugin(PluginCode).(*Plugin)
	dbStats := plugin.GetMySQLQueriesStats(false)
	assert.NotEmpty(t, dbStats)
	slowStats := plugin.GetMySQLSlowQueriesStats()

	assert.NotEmpty(t, slowStats)
	assert.Len(t, slowStats, 5)
	assert.GreaterOrEqual(t, slowStats[0].Duration, slowStats[1].Duration)
	assert.Equal(t, "default", slowStats[0].Pool)

	plugin.ClearMySQLStats()
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Len(t, dbStats, 0)
	slowStats = plugin.GetMySQLSlowQueriesStats()
	assert.Len(t, slowStats, 5)
	plugin.ClearMySQLSlowQueries()
	slowStats = plugin.GetMySQLSlowQueriesStats()
	assert.Len(t, slowStats, 0)

	entity = &simpleMetricsEntity{Name: "One"}
	engine.Flush(entity)
	entity = &simpleMetricsEntity{Name: "Two"}
	engine.Flush(entity)
	val := ""
	engine.GetMysql().QueryRow(beeorm.NewWhere("SELECT 1"), &val)
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Len(t, dbStats, 2)
	assert.Equal(t, Insert, dbStats[0].Operation)
	assert.Equal(t, "simplemetricsentity", dbStats[0].Table)
	assert.Equal(t, uint64(2), dbStats[0].Counter)
	assert.Greater(t, dbStats[0].TotalTime, dbStats[1].TotalTime)
	slowStats = plugin.GetMySQLSlowQueriesStats()
	assert.Len(t, slowStats, 3)

	plugin.ClearMySQLStats()

	entity = &simpleMetricsEntity{Name: "Three"}
	engine.FlushLazy(entity)
	beeorm.RunLazyFlushConsumer(engine, false)
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Len(t, dbStats, 0)
	dbStats = plugin.GetMySQLQueriesStats(true)
	assert.Len(t, dbStats, 1)
	assert.Equal(t, Insert, dbStats[0].Operation)
	assert.Equal(t, "simplemetricsentity", dbStats[0].Table)
	assert.Equal(t, uint64(1), dbStats[0].Counter)

	entity = &simpleMetricsEntity{}
	engine.LoadByID(1, entity)
	plugin.ClearMySQLStats()
	entity.Name = "OneV2"
	engine.Flush(entity)
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Len(t, dbStats, 1)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, Update, dbStats[0].Operation)

	plugin.ClearMySQLStats()

	engine.Delete(entity)
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, Delete, dbStats[0].Operation)

	plugin.ClearMySQLStats()
	engine.LoadByID(2, entity)
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, Query, dbStats[0].Operation)

	plugin.ClearMySQLStats()
	date := ""
	engine.GetMysql().QueryRow(beeorm.NewWhere("SELECT NOW();"), &date)
	dbStats = plugin.GetMySQLQueriesStats(false)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, Query, dbStats[0].Operation)
	assert.Equal(t, "unknown", dbStats[0].Table)

	plugin.ClearMySQLStats()
	engine.GetMysql().QueryRow(beeorm.NewWhere("SELECT 1"), &date)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, Query, dbStats[0].Operation)
	assert.Equal(t, "unknown", dbStats[0].Table)
}
