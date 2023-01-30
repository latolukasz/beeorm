package log_tables

import (
	"context"
	"github.com/latolukasz/beeorm"
	"github.com/latolukasz/beeorm/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

type logReceiverEntity1 struct {
	beeorm.ORM `orm:"log=log;redisCache"`
	Name       string `orm:"unique=name"`
	LastName   string
	Country    string `orm:"skip-table-log"`
}

type logReceiverEntity2 struct {
	beeorm.ORM `orm:"redisCache;log"`
	Name       string
	Age        uint64
}

type logReceiverEntity3 struct {
	beeorm.ORM `orm:"log=log"`
	Name       string
	Age        uint64
}

type logReceiverEntity4 struct {
	beeorm.ORM
	Name string
	Age  uint64
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
	registry.RegisterPlugin(Init())
	engine := test.PrepareTables(t, registry, MySQLVersion, 7, "", entity1, entity2, entity3, entity4)
	engine.GetMysql("log").Exec("TRUNCATE TABLE `_log_log_logReceiverEntity1`")
	engine.GetMysql().Exec("TRUNCATE TABLE `_log_default_logReceiverEntity2`")
	engine.GetMysql("log").Exec("TRUNCATE TABLE `_log_log_logReceiverEntity3`")
	engine.GetRedis().FlushDB()

	e1 := &logReceiverEntity1{Name: "John", LastName: "Smith", Country: "Poland"}
	engine.Flush(e1)
	e2 := &logReceiverEntity2{Name: "Tom", Age: 18}
	engine.Flush(e2)

	statistics := engine.GetEventBroker().GetStreamGroupStatistics(LogTablesChannelName, LogTablesConsumerGroupName)
	assert.Equal(t, int64(2), engine.GetRedis().XLen(LogTablesChannelName))
	assert.Equal(t, uint64(0), statistics.Pending)

	consumer := engine.GetEventBroker().Consumer(LogTablesConsumerGroupName)
	consumer.SetBlockTime(0)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))

	assert.Equal(t, int64(2), engine.GetRedis().XLen(LogTablesChannelName))

	schema := engine.GetRegistry().GetTableSchemaForEntity(entity1)
	logs := GetEntityLogs(engine, schema, 1, nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].Meta)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].Changes)
	assert.Equal(t, uint64(1), logs[0].LogID)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Equal(t, "John", logs[0].Changes["Name"])
	assert.Equal(t, "Poland", logs[0].Changes["Country"])
	assert.Equal(t, "Smith", logs[0].Changes["LastName"])
	test.RunLazyFlushConsumer(engine, true)
	assert.Equal(t, int64(0), engine.GetRedis().XLen(LogTablesChannelName))

	schema2 := engine.GetRegistry().GetTableSchemaForEntity(entity2)
	logs = GetEntityLogs(engine, schema2, 1, nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].Meta)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].Changes)
	assert.Equal(t, uint64(1), logs[0].LogID)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Equal(t, "18", logs[0].Changes["Age"])
	assert.Equal(t, "Tom", logs[0].Changes["Name"])

	SetMetaData(engine, "user_id", "12")
	SetMetaData(engine, "country", "Poland")
	flusher := engine.NewFlusher()
	e := &logReceiverEntity1{Name: "John2"}
	e3 := &logReceiverEntity1{Name: "John3"}
	flusher.Track(e, e3)
	flusher.Flush()
	assert.Equal(t, int64(2), engine.GetRedis().XLen(LogTablesChannelName))

	consumer.Consume(context.Background(), 100, NewEventHandler(engine))

	logs = GetEntityLogs(engine, schema, e.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.NotNil(t, logs[0].Meta)
	assert.Len(t, logs[0].Meta, 2)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].Changes)
	assert.Equal(t, "John2", logs[0].Changes["Name"])
	assert.Equal(t, "12", logs[0].Meta["user_id"])
	assert.Equal(t, "Poland", logs[0].Meta["country"])

	logs = GetEntityLogs(engine, schema, e3.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.NotNil(t, logs[0].Meta)
	assert.Len(t, logs[0].Meta, 2)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].Changes)
	assert.Equal(t, "John3", logs[0].Changes["Name"])
	assert.Equal(t, "12", logs[0].Meta["user_id"])
	assert.Equal(t, "Poland", logs[0].Meta["country"])

	e1.Country = "Germany"
	engine.Flush(e1)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))

	logs = GetEntityLogs(engine, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 2)
	assert.NotNil(t, logs[1].Before)
	assert.NotNil(t, logs[1].Changes)
	assert.Len(t, logs[1].Before, 1)
	assert.Len(t, logs[1].Changes, 1)
	assert.Equal(t, "Poland", logs[1].Before["Country"])
	assert.Equal(t, "Germany", logs[1].Changes["Country"])

	engine.Delete(e1)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))
	logs = GetEntityLogs(engine, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 3)
	assert.NotNil(t, logs[2].Before)
	assert.Nil(t, logs[2].Changes)
	assert.Len(t, logs[2].Before, 3)
	assert.Equal(t, "Germany", logs[2].Before["Country"])
	assert.Equal(t, "John", logs[2].Before["Name"])
	assert.Equal(t, "Smith", logs[2].Before["LastName"])

	// TODO insert on duplikate

	e1 = &logReceiverEntity1{Name: "Duplicate"}
	e1.SetOnDuplicateKeyUpdate(beeorm.Bind{"LastName": "Duplicated"})
	engine.Flush(e1)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))
	logs = GetEntityLogs(engine, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 1)
	assert.Nil(t, logs[0].Before)
	assert.NotNil(t, logs[0].Changes)
	assert.Len(t, logs[0].Changes, 3)
	assert.Equal(t, "Duplicate", logs[0].Changes["Name"])
	assert.Equal(t, "NULL", logs[0].Changes["LastName"])
	assert.Equal(t, "NULL", logs[0].Changes["Country"])

	e1 = &logReceiverEntity1{Name: "Duplicate"}
	e1.SetOnDuplicateKeyUpdate(beeorm.Bind{"LastName": "Duplicated last name"})
	engine.Flush(e1)
	consumer.Consume(context.Background(), 100, NewEventHandler(engine))
	logs = GetEntityLogs(engine, schema, e1.GetID(), nil, nil)
	assert.Len(t, logs, 2)
	assert.Nil(t, logs[1].Before)
	assert.NotNil(t, logs[1].Changes)
	assert.Len(t, logs[1].Changes, 1)
	assert.Equal(t, "Duplicated last name", logs[1].Changes["LastName"])

	//logs = schema.GetEntityLogs(engine, 2, nil, orm.NewWhere("`ID` = ?", 4))
	//assert.Len(t, logs, 1)
	//assert.NotNil(t, logs[0].Meta)
	//assert.Equal(t, float64(12), logs[0].Meta["user_id"])
	//assert.Nil(t, logs[0].Changes)
	//assert.Equal(t, "John2", logs[0].Before["Name"])
	//assert.Equal(t, "Germany", logs[0].Before["Country"])
	//assert.Equal(t, "Summer", logs[0].Before["LastName"])
	//
	//e3 := &logReceiverEntity1{Name: "Adam", LastName: "Pol", Country: "Brazil"}
	//engine.FlushLazy(e3)
	//receiver := orm.NewBackgroundConsumer(engine)
	//receiver.DisableBlockMode()
	////receiver.blockTime = time.Millisecond
	//receiver.Digest(context.Background())
	//
	//logs = schema.GetEntityLogs(engine, 3, nil, nil)
	//assert.Len(t, logs, 1)
	//assert.NotNil(t, logs[0].Changes)
	//assert.Nil(t, logs[0].Before)
	//assert.NotNil(t, logs[0].Meta)
	//assert.Equal(t, uint64(3), logs[0].EntityID)
	//assert.Equal(t, "Adam", logs[0].Changes["Name"])
	//assert.Equal(t, "Brazil", logs[0].Changes["Country"])
	//assert.Equal(t, "Pol", logs[0].Changes["LastName"])
	//
	//engine.LoadByID(3, e3)
	//e3.Name = "Eva"
	//engine.FlushLazy(e3)
	//receiver.Digest(context.Background())
	//
	//logs = schema.GetEntityLogs(engine, 3, nil, nil)
	//assert.Len(t, logs, 2)
	//assert.NotNil(t, logs[1].Changes)
	//assert.NotNil(t, logs[1].Before)
	//assert.NotNil(t, logs[1].Meta)
	//assert.Equal(t, uint64(3), logs[1].EntityID)
	//assert.Equal(t, "Eva", logs[1].Changes["Name"])
	//assert.Equal(t, "Adam", logs[1].Before["Name"])
	//assert.Equal(t, "Brazil", logs[1].Before["Country"])
	//assert.Equal(t, "Pol", logs[1].Before["LastName"])
	//
	//engine.LoadByID(3, e3)
	//flusher = engine.NewFlusher()
	//flusher.Delete(e3)
	//flusher.FlushLazy()
	//receiver.Digest(context.Background())
	//
	//logs = schema.GetEntityLogs(engine, 3, nil, nil)
	//assert.Len(t, logs, 3)
	//assert.Nil(t, logs[2].Changes)
	//assert.NotNil(t, logs[2].Before)
	//assert.NotNil(t, logs[2].Meta)
	//assert.Equal(t, uint64(3), logs[2].EntityID)
	//assert.Equal(t, "Eva", logs[2].Before["Name"])
	//assert.Equal(t, "Brazil", logs[2].Before["Country"])
	//assert.Equal(t, "Pol", logs[2].Before["LastName"])
	//
	//logs = schema.GetEntityLogs(engine, 3, nil, orm.NewWhere("ID IN ? ORDER BY ID DESC", []int{4, 5, 6}))
	//assert.Len(t, logs, 2)
	//assert.Equal(t, uint64(6), logs[0].LogID)
	//assert.Equal(t, uint64(5), logs[1].LogID)
	//
	//logs = schema.GetEntityLogs(engine, 3, orm.NewPager(2, 2), nil)
	//assert.Len(t, logs, 1)
	//assert.Equal(t, uint64(7), logs[0].LogID)
	//
	//logs = engine.GetRegistry().GetTableSchemaForEntity(entity3).GetEntityLogs(engine, 1, nil, nil)
	//assert.Len(t, logs, 0)
	//
	//e4 := &logReceiverEntity2{}
	//e5 := &logReceiverEntity2{}
	//engine.LoadByID(1, e4)
	//engine.LoadByID(2, e5)
	//flusher = engine.NewFlusher()
	//engine.GetMysql().Begin()
	//e4.Age = 34
	//flusher.Track(e4)
	//_ = flusher.FlushWithCheck()
	//_ = flusher.FlushWithCheck()
	//e5.Name = "Lucas"
	//flusher.Track(e5)
	//_ = flusher.FlushWithCheck()
	//logger := &testLogHandler{}
	//engine.RegisterQueryLogger(logger, true, true, false)
	//engine.GetMysql().Commit()
	//assert.Len(t, logger.Logs, 2)
	//assert.Equal(t, "COMMIT", logger.Logs[0]["operation"])
	//assert.Equal(t, "PIPELINE EXEC", logger.Logs[1]["operation"])
	//
	//engine.GetMysql().Begin()
	//flusher = engine.NewFlusher()
	//flusher.Flush()
	//e4.Age = 100
	//flusher.Track(e4)
	//flusher.Flush()
	//flusher = engine.NewFlusher()
	//flusher.Track(e4)
	//flusher.Flush()
	//logger.clear()
	//engine.GetMysql().Commit()
	//assert.Len(t, logger.Logs, 2)
	//assert.Equal(t, "COMMIT", logger.Logs[0]["operation"])
	//assert.Equal(t, "PIPELINE EXEC", logger.Logs[1]["operation"])
	//
	//engine.LoadByID(2, e1)
	//e1.LastName = "Winter"
	//engine.Flush(e1)
	//engine.GetMysql("log").Exec("DROP TABLE `_log_default_logReceiverEntity1`")
	//assert.NotPanics(t, func() {
	//	receiver.Digest(context.Background())
	//})
	//statistics = engine.GetEventBroker().GetStreamGroupStatistics(LogTablesChannelName, orm.BackgroundConsumerGroupName)
	//if redisVersion == 7 {
	//	assert.Equal(t, int64(0), statistics.Lag)
	//}
	//assert.Equal(t, uint64(0), statistics.Pending)
}
