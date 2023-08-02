package simple_metrics

import (
	"testing"

	"github.com/latolukasz/beeorm/v3"
	"github.com/stretchr/testify/assert"
)

type simpleMetricsEntity struct {
	beeorm.ORM
	ID   uint64
	Name string `orm:"required"`
}

func TestMysqlMetrics(t *testing.T) {
	registry := &beeorm.Registry{}
	var entity *simpleMetricsEntity

	plugin := Init(&Options{MySQLSlowQueriesLimit: 5})
	registry.RegisterPlugin(plugin)
	c := beeorm.PrepareTables(t, registry, 8, 6, "", entity)

	plugin = c.Engine().Registry().Plugin(PluginCode).(*Plugin)
	dbStats := plugin.GetMySQLQueriesStats("")
	assert.NotEmpty(t, dbStats)
	slowStats := plugin.GetMySQLSlowQueriesStats("")

	assert.NotEmpty(t, slowStats)
	assert.Len(t, slowStats, 5)
	assert.GreaterOrEqual(t, slowStats[0].Duration, slowStats[1].Duration)
	assert.Equal(t, "default", slowStats[0].Pool)

	plugin.ClearMySQLStats()
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Len(t, dbStats, 0)
	slowStats = plugin.GetMySQLSlowQueriesStats("")
	assert.Len(t, slowStats, 0)

	entity = &simpleMetricsEntity{Name: "One"}
	c.Flusher().Track(entity).Flush()
	entity = &simpleMetricsEntity{Name: "Two"}
	c.Flusher().Track(entity).Flush()
	val := ""
	c.Engine().DB().QueryRow(c, beeorm.NewWhere("SELECT 1"), &val)
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Len(t, dbStats, 2)
	assert.Equal(t, INSERT, dbStats[0].Operation)
	assert.Equal(t, "simplemetricsentity", dbStats[0].Table)
	assert.Equal(t, uint64(2), dbStats[0].Counter)
	assert.Greater(t, dbStats[0].TotalTime, dbStats[1].TotalTime)
	slowStats = plugin.GetMySQLSlowQueriesStats("")
	assert.Len(t, slowStats, 3)
	assert.Len(t, plugin.GetTags(), 1)
	assert.Equal(t, "", plugin.GetTags()[0])

	plugin.ClearMySQLStats()

	entity = &simpleMetricsEntity{Name: "Three"}
	c.Flusher().Track(entity).FlushLazy()
	beeorm.RunLazyFlushConsumer(c, false)
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Len(t, dbStats, 0)
	dbStats = plugin.GetMySQLQueriesStats("lazy")
	assert.Len(t, dbStats, 1)
	assert.Equal(t, INSERT, dbStats[0].Operation)
	assert.Equal(t, "simplemetricsentity", dbStats[0].Table)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Len(t, plugin.GetTags(), 1)
	assert.Equal(t, "lazy", plugin.GetTags()[0])
	slowStats = plugin.GetMySQLSlowQueriesStats("lazy")
	assert.Len(t, slowStats, 1)

	entity = &simpleMetricsEntity{}
	beeorm.GetByID[*simpleMetricsEntity](c, 1)
	plugin.ClearMySQLStats()
	entity.Name = "OneV2"
	c.Flusher().Track(entity).Flush()
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Len(t, dbStats, 1)
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, UPDATE, dbStats[0].Operation)

	plugin.ClearMySQLStats()

	c.Flusher().Delete(entity).Flush()
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, DELETE, dbStats[0].Operation)

	plugin.ClearMySQLStats()
	beeorm.GetByID[*simpleMetricsEntity](c, 2)
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, QUERY, dbStats[0].Operation)

	plugin.ClearMySQLStats()
	date := ""
	c.Engine().DB().QueryRow(c, beeorm.NewWhere("SELECT NOW();"), &date)
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, QUERY, dbStats[0].Operation)
	assert.Equal(t, "unknown", dbStats[0].Table)

	plugin.ClearMySQLStats()
	c.Engine().DB().QueryRow(c, beeorm.NewWhere("SELECT 1"), &date)
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Equal(t, uint64(1), dbStats[0].Counter)
	assert.Equal(t, QUERY, dbStats[0].Operation)
	assert.Equal(t, "unknown", dbStats[0].Table)
	SetTagName(c, "my_tag")
	c.Engine().DB().QueryRow(c, beeorm.NewWhere("SELECT 1"), &date)
	assert.Len(t, plugin.GetTags(), 2)
	assert.Equal(t, plugin.GetTags()[0], "")
	assert.Equal(t, plugin.GetTags()[1], "my_tag")

	plugin.ClearMySQLStats()
	DisableMetrics(c)
	c.Engine().DB().QueryRow(c, beeorm.NewWhere("SELECT 1"), &date)
	dbStats = plugin.GetMySQLQueriesStats("")
	assert.Len(t, dbStats, 0)
}
