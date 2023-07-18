package log_table

import (
	"context"
	"testing"

	"github.com/latolukasz/beeorm/v3/plugins/crud_stream"

	"github.com/latolukasz/beeorm/v3"
	"github.com/stretchr/testify/assert"
)

type logReceiverEntity1 struct {
	beeorm.ORM `orm:"crud-stream;log-table=log;redisCache"`
	ID         uint64
	Name       string
	LastName   string
	Country    string
}

type logReceiverEntity2 struct {
	beeorm.ORM `orm:"crud-stream;redisCache;log-table"`
	ID         uint64
	Name       string
	Age        uint64
}

type logReceiverEntity3 struct {
	beeorm.ORM `orm:"crud-stream;log-table=log"`
	ID         uint64
	Name       string
	Age        uint64
}

type logReceiverEntity4 struct {
	beeorm.ORM `orm:"crud-stream"`
	ID         uint64
	Name       string
	Age        uint64
}

func TestLogReceiverMySQL5(t *testing.T) {
	testLogReceiver(t, 5)
}

func TestLogReceiverMySQL8(t *testing.T) {
	testLogReceiver(t, 8)
}

func testLogReceiver(t *testing.T, MySQLVersion int) {
	var entity1 *logReceiverEntity1
	var entity2 *logReceiverEntity2
	var entity3 *logReceiverEntity3
	var entity4 *logReceiverEntity4
	registry := &beeorm.Registry{}
	registry.RegisterPlugin(crud_stream.Init(nil))
	registry.RegisterPlugin(Init(nil))
	engine := beeorm.PrepareTables(t, registry, MySQLVersion, 7, "", entity1, entity2, entity3, entity4)
	assert.Len(t, engine.GetAlters(), 0)
	engine.GetMysql("log").Exec("TRUNCATE TABLE `_log_default_logReceiverEntity1`")
	engine.GetMysql().Exec("TRUNCATE TABLE `_log_default_logReceiverEntity2`")
	engine.GetMysql("log").Exec("TRUNCATE TABLE `_log_default_logReceiverEntity3`")
	engine.GetRedis().FlushDB()

	e1 := &logReceiverEntity1{Name: "John", LastName: "Smith", Country: "Poland"}
	engine.Flush(e1)
	e2 := &logReceiverEntity2{Name: "Tom", Age: 18}
	engine.Flush(e2)

	consumer := engine.GetEventBroker().Consumer(ConsumerGroupName)
	consumer.SetBlockTime(0)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))

	assert.Equal(t, int64(2), engine.GetRedis().XLen(crud_stream.ChannelName))
	beeorm.RunStreamGarbageCollectorConsumer(engine)
	assert.Equal(t, int64(0), engine.GetRedis().XLen(crud_stream.ChannelName))

	schema := engine.Registry().GetEntitySchemaForEntity(entity1)
	logs := GetEntityLogs(engine, schema, 1, nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].MetaData)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, uint64(1), logs[0].LogID)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Equal(t, "John", logs[0].After["Name"])
	assert.Equal(t, "Poland", logs[0].After["Country"])
	assert.Equal(t, "Smith", logs[0].After["LastName"])
	beeorm.RunStreamGarbageCollectorConsumer(engine)
	assert.Equal(t, int64(0), engine.GetRedis().XLen(crud_stream.ChannelName))

	schema2 := engine.Registry().GetEntitySchemaForEntity(entity2)
	logs = GetEntityLogs(engine, schema2, 1, nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].MetaData)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, uint64(1), logs[0].LogID)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Equal(t, "18", logs[0].After["Age"])
	assert.Equal(t, "Tom", logs[0].After["Name"])

	engine.SetMetaData("user_id", "12")
	engine.SetMetaData("country", "Poland")
	flusher := engine.NewFlusher()
	e := &logReceiverEntity1{Name: "John2"}
	e3 := &logReceiverEntity1{Name: "John3"}
	flusher.Track(e, e3)
	flusher.Flush()
	assert.Equal(t, int64(2), engine.GetRedis().XLen(crud_stream.ChannelName))

	consumer.Consume(context.Background(), 100, NewEventHandler(engine))

	logs = GetEntityLogs(engine, schema, e.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.NotNil(t, logs[0].MetaData)
	assert.Len(t, logs[0].MetaData, 2)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, "John2", logs[0].After["Name"])
	assert.Equal(t, "12", logs[0].MetaData["user_id"])
	assert.Equal(t, "Poland", logs[0].MetaData["country"])

	logs = GetEntityLogs(engine, schema, e3.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.NotNil(t, logs[0].MetaData)
	assert.Len(t, logs[0].MetaData, 2)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, "John3", logs[0].After["Name"])
	assert.Equal(t, "12", logs[0].MetaData["user_id"])
	assert.Equal(t, "Poland", logs[0].MetaData["country"])

	e1.Country = "Germany"
	engine.Flush(e1)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))

	logs = GetEntityLogs(engine, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 2)
	assert.NotNil(t, logs[1].Before)
	assert.NotNil(t, logs[1].After)
	assert.Len(t, logs[1].Before, 1)
	assert.Len(t, logs[1].After, 1)
	assert.Equal(t, "Poland", logs[1].Before["Country"])
	assert.Equal(t, "Germany", logs[1].After["Country"])

	engine.Delete(e1)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))
	logs = GetEntityLogs(engine, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 3)
	assert.NotNil(t, logs[2].Before)
	assert.Nil(t, logs[2].After)
	assert.Len(t, logs[2].Before, 4)
	assert.Equal(t, "Germany", logs[2].Before["Country"])
	assert.Equal(t, "John", logs[2].Before["Name"])
	assert.Equal(t, "Smith", logs[2].Before["LastName"])

	logs = GetEntityLogs(engine, schema, 1, nil, beeorm.NewWhere("`before`IS NOT NULL ORDER BY ID DESC"))
	assert.Len(t, logs, 2)
	assert.Len(t, logs[0].Before, 4)
	assert.Len(t, logs[1].Before, 1)

	logs = GetEntityLogs(engine, schema, 1, beeorm.NewPager(2, 1), beeorm.NewWhere("`before`IS NOT NULL ORDER BY ID DESC"))
	assert.Len(t, logs, 1)
	assert.Len(t, logs[0].Before, 1)

	logs = GetEntityLogs(engine, engine.Registry().GetEntitySchemaForEntity(entity3), 1, nil, nil)
	assert.Len(t, logs, 0)

	engine.LoadByID(2, e1)
	e1.LastName = "Winter"
	engine.Flush(e1)
	engine.GetMysql("log").Exec("DROP TABLE `_log_default_logReceiverEntity1`")
	assert.NotPanics(t, func() {
		consumer.Consume(context.Background(), 100, NewEventHandler(engine))
	})
}
