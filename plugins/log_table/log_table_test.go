package log_table

import (
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
	c := beeorm.PrepareTables(t, registry, MySQLVersion, 7, "", entity1, entity2, entity3, entity4)
	assert.Len(t, beeorm.GetAlters(c), 0)
	c.Engine().GetMySQLByCode("log").Exec(c, "TRUNCATE TABLE `_log_default_logReceiverEntity1`")
	c.Engine().GetMySQL().Exec(c, "TRUNCATE TABLE `_log_default_logReceiverEntity2`")
	c.Engine().GetMySQLByCode("log").Exec(c, "TRUNCATE TABLE `_log_default_logReceiverEntity3`")
	c.Engine().GetRedis("").FlushDB(c)

	e1 := &logReceiverEntity1{Name: "John", LastName: "Smith", Country: "Poland"}
	c.Flusher().Track(e1).Flush()
	e2 := &logReceiverEntity2{Name: "Tom", Age: 18}
	c.Flusher().Track(e2).Flush()

	consumer := c.EventBroker().Consumer(ConsumerGroupName)
	consumer.SetBlockTime(0)
	consumer.Consume(100, NewEventHandler(c))

	assert.Equal(t, int64(2), c.Engine().GetRedis("").XLen(c, crud_stream.ChannelName))
	beeorm.RunStreamGarbageCollectorConsumer(c)
	assert.Equal(t, int64(0), c.Engine().GetRedis("").XLen(c, crud_stream.ChannelName))

	schema := beeorm.GetEntitySchema[*logReceiverEntity1](c)
	logs := GetEntityLogs(c, schema, 1, nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].MetaData)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, uint64(1), logs[0].LogID)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Equal(t, "John", logs[0].After["Name"])
	assert.Equal(t, "Poland", logs[0].After["Country"])
	assert.Equal(t, "Smith", logs[0].After["LastName"])
	beeorm.RunStreamGarbageCollectorConsumer(c)
	assert.Equal(t, int64(0), c.Engine().GetRedis("").XLen(c, crud_stream.ChannelName))

	schema2 := beeorm.GetEntitySchema[*logReceiverEntity2](c)
	logs = GetEntityLogs(c, schema2, 1, nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].MetaData)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, uint64(1), logs[0].LogID)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Equal(t, "18", logs[0].After["Age"])
	assert.Equal(t, "Tom", logs[0].After["Name"])

	c.SetMetaData("user_id", "12")
	c.SetMetaData("country", "Poland")
	flusher := c.Flusher()
	e := &logReceiverEntity1{Name: "John2"}
	e3 := &logReceiverEntity1{Name: "John3"}
	flusher.Track(e, e3)
	flusher.Flush()
	assert.Equal(t, int64(2), c.Engine().GetRedis("").XLen(c, crud_stream.ChannelName))

	consumer.Consume(100, NewEventHandler(c))

	logs = GetEntityLogs(c, schema, e.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.NotNil(t, logs[0].MetaData)
	assert.Len(t, logs[0].MetaData, 2)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, "John2", logs[0].After["Name"])
	assert.Equal(t, "12", logs[0].MetaData["user_id"])
	assert.Equal(t, "Poland", logs[0].MetaData["country"])

	logs = GetEntityLogs(c, schema, e3.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.NotNil(t, logs[0].MetaData)
	assert.Len(t, logs[0].MetaData, 2)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].After)
	assert.Equal(t, "John3", logs[0].After["Name"])
	assert.Equal(t, "12", logs[0].MetaData["user_id"])
	assert.Equal(t, "Poland", logs[0].MetaData["country"])

	e1.Country = "Germany"
	c.Flusher().Track(e1).Flush()
	consumer.Consume(100, NewEventHandler(c))

	logs = GetEntityLogs(c, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 2)
	assert.NotNil(t, logs[1].Before)
	assert.NotNil(t, logs[1].After)
	assert.Len(t, logs[1].Before, 1)
	assert.Len(t, logs[1].After, 1)
	assert.Equal(t, "Poland", logs[1].Before["Country"])
	assert.Equal(t, "Germany", logs[1].After["Country"])

	c.Flusher().Delete(e1).Flush()
	consumer.Consume(100, NewEventHandler(c))
	logs = GetEntityLogs(c, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 3)
	assert.NotNil(t, logs[2].Before)
	assert.Nil(t, logs[2].After)
	assert.Len(t, logs[2].Before, 4)
	assert.Equal(t, "Germany", logs[2].Before["Country"])
	assert.Equal(t, "John", logs[2].Before["Name"])
	assert.Equal(t, "Smith", logs[2].Before["LastName"])

	logs = GetEntityLogs(c, schema, 1, nil, beeorm.NewWhere("`before`IS NOT NULL ORDER BY ID DESC"))
	assert.Len(t, logs, 2)
	assert.Len(t, logs[0].Before, 4)
	assert.Len(t, logs[1].Before, 1)

	logs = GetEntityLogs(c, schema, 1, beeorm.NewPager(2, 1), beeorm.NewWhere("`before`IS NOT NULL ORDER BY ID DESC"))
	assert.Len(t, logs, 1)
	assert.Len(t, logs[0].Before, 1)

	logs = GetEntityLogs(c, beeorm.GetEntitySchema[*logReceiverEntity3](c), 1, nil, nil)
	assert.Len(t, logs, 0)

	beeorm.GetByID[*logReceiverEntity1](c, 2)
	e1.LastName = "Winter"
	c.Flusher().Track(e1).Flush()
	c.Engine().GetMySQLByCode("log").Exec(c, "DROP TABLE `_log_default_logReceiverEntity1`")
	assert.NotPanics(t, func() {
		consumer.Consume(100, NewEventHandler(c))
	})
}
