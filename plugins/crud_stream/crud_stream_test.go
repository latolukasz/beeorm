package crud_stream

import (
	"context"
	"testing"
	"time"

	"github.com/latolukasz/beeorm/v3"
	"github.com/stretchr/testify/assert"
)

type crudStreamEntity struct {
	beeorm.ORM `orm:"crud-stream;redisCache"`
	ID         uint64
	Name       string `orm:"unique=name"`
	LastName   string
	Country    string `orm:"skip-crud-stream"`
}

func TestCrudStream(t *testing.T) {
	var entity *crudStreamEntity

	registry := &beeorm.Registry{}
	registry.RegisterPlugin(Init(nil))
	registry.RegisterRedisStreamConsumerGroups(ChannelName, "test-consumer")
	c := beeorm.PrepareTables(t, registry, 8, 7, "", entity)

	e1 := &crudStreamEntity{Name: "John", LastName: "Smith", Country: "Germany"}
	c.Flusher().Track(e1)
	e2 := &crudStreamEntity{Name: "Adam", LastName: "Kowalski", Country: "Poland"}
	c.Flusher().Track(e2).Flush()

	consumer := c.EventBroker().Consumer("test-consumer")
	consumer.SetBlockTime(0)
	valid := false
	consumer.Consume(2, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 2)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Insert, crudEvent.Action)
		assert.Len(t, events[0].Meta(), 0)
		assert.Nil(t, crudEvent.Before)
		assert.Len(t, crudEvent.Changes, 4)
		assert.Equal(t, "Germany", crudEvent.Changes["Country"])
		assert.Equal(t, "Smith", crudEvent.Changes["LastName"])
		assert.Equal(t, "John", crudEvent.Changes["Name"])
		assert.Equal(t, uint64(1), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))

		events[1].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Insert, crudEvent.Action)
		assert.Len(t, events[0].Meta(), 0)
		assert.Nil(t, crudEvent.Before)
		assert.Len(t, crudEvent.Changes, 4)
		assert.Equal(t, "Poland", crudEvent.Changes["Country"])
		assert.Equal(t, "Kowalski", crudEvent.Changes["LastName"])
		assert.Equal(t, "Adam", crudEvent.Changes["Name"])
		assert.Equal(t, uint64(2), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))
	})
	assert.True(t, valid)

	e1.Name = "Tom"
	c.Flusher().Track(e1).Flush()

	valid = false
	consumer.Consume(2, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 1)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Update, crudEvent.Action)
		assert.Len(t, events[0].Meta(), 0)
		assert.Len(t, crudEvent.Before, 1)
		assert.Len(t, crudEvent.Changes, 1)
		assert.Equal(t, "Tom", crudEvent.Changes["Name"])
		assert.Equal(t, "John", crudEvent.Before["Name"])
		assert.Equal(t, uint64(1), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))
	})
	assert.True(t, valid)

	e1.Country = "France"
	c.Flusher().Track(e1).Flush()
	valid = false
	consumer.Consume(2, func(events []beeorm.Event) {
		valid = true
	})
	assert.False(t, valid)

	c.Flusher().Delete(e1).Flush()
	valid = false
	consumer.Consume(2, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 1)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Delete, crudEvent.Action)
		assert.Len(t, events[0].Meta(), 0)
		assert.Len(t, crudEvent.Before, 4)
		assert.Nil(t, crudEvent.Changes)
		assert.Equal(t, "France", crudEvent.Before["Country"])
		assert.Equal(t, "Smith", crudEvent.Before["LastName"])
		assert.Equal(t, "Tom", crudEvent.Before["Name"])
		assert.Equal(t, uint64(1), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))
	})
	assert.True(t, valid)

	e1 = &crudStreamEntity{Name: "Ivona", LastName: "Summer", Country: "France"}
	e1.SetOnDuplicateKeyUpdate(beeorm.Bind{"LastName": "Spring"})
	c.Flusher().Track(e1).Flush()

	valid = false
	consumer.Consume(2, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 1)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Insert, crudEvent.Action)
		assert.Len(t, events[0].Meta(), 0)
		assert.Nil(t, crudEvent.Before)
		assert.Len(t, crudEvent.Changes, 4)
		assert.Equal(t, "France", crudEvent.Changes["Country"])
		assert.Equal(t, "Ivona", crudEvent.Changes["Name"])
		assert.Equal(t, "Summer", crudEvent.Changes["LastName"])
		assert.Equal(t, uint64(3), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))
	})
	assert.True(t, valid)

	e1 = &crudStreamEntity{Name: "Ivona", LastName: "Summer", Country: "France"}
	e1.SetOnDuplicateKeyUpdate(beeorm.Bind{"LastName": "Spring"})
	c.Flusher().Track(e1).Flush()

	valid = false
	consumer.Consume(2, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 1)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Update, crudEvent.Action)
		assert.Len(t, events[0].Meta(), 0)
		assert.Nil(t, crudEvent.Before)
		assert.Len(t, crudEvent.Changes, 1)
		assert.Equal(t, "Spring", crudEvent.Changes["LastName"])
		assert.Equal(t, uint64(3), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))
	})
	assert.True(t, valid)

	c.SetMetaData("source", "test")
	c.SetMetaData("user", "me")
	e1 = &crudStreamEntity{Name: "Hugo", LastName: "Winter", Country: "Poland"}
	c.Flusher().Track(e1).Flush()
	valid = false
	consumer.Consume(1, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 1)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Insert, crudEvent.Action)
		assert.Len(t, crudEvent.MetaData, 2)
		assert.Equal(t, "test", crudEvent.MetaData.Get("source"))
		assert.Equal(t, "me", crudEvent.MetaData.Get("user"))
	})
	assert.True(t, valid)

	e1 = &crudStreamEntity{Name: "Veronica", LastName: "Snow", Country: "Spain"}
	c.Flusher().Track(e1).FlushLazy()
	valid = false
	consumer.Consume(1, func(events []beeorm.Event) {
		valid = true
	})
	assert.False(t, valid)

	beeorm.RunLazyFlushConsumer(c.Engine().NewContext(context.Background()), true)
	valid = false
	consumer = c.Engine().NewContext(context.Background()).EventBroker().Consumer("test-consumer")
	consumer.SetBlockTime(0)
	consumer.Consume(10, func(events []beeorm.Event) {
		valid = true
		assert.Len(t, events, 1)
		var crudEvent CrudEvent
		events[0].Unserialize(&crudEvent)
		assert.Equal(t, beeorm.Insert, crudEvent.Action)
		assert.Len(t, crudEvent.MetaData, 3)
		assert.Equal(t, "1", crudEvent.MetaData.Get("lazy"))
		assert.Equal(t, "test", crudEvent.MetaData.Get("source"))
		assert.Equal(t, "me", crudEvent.MetaData.Get("user"))
		assert.Equal(t, "", crudEvent.MetaData.Get("invalid"))
		assert.Nil(t, crudEvent.Before)
		assert.Len(t, crudEvent.Changes, 4)
		assert.Equal(t, "Spain", crudEvent.Changes["Country"])
		assert.Equal(t, "Snow", crudEvent.Changes["LastName"])
		assert.Equal(t, "Veronica", crudEvent.Changes["Name"])
		assert.Equal(t, uint64(6), crudEvent.ID)
		assert.Equal(t, "crud_stream.crudStreamEntity", crudEvent.EntityName)
		assert.LessOrEqual(t, time.Now().Unix()-crudEvent.Updated.Unix(), int64(5))
	})
	assert.True(t, valid)
}
