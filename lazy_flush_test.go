package beeorm

import (
	"context"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

type lazyReceiverEntity struct {
	ORM          `orm:"localCache;redisCache;asyncRedisLazyFlush=default"`
	ID           uint64
	Name         string `orm:"unique=name"`
	Age          uint64
	EnumNullable string `orm:"enum=TestEnum"`
	RefOne       *lazyReceiverReference
	IndexAll     *CachedQuery `query:""`
}

type lazyReceiverReference struct {
	ORM
	ID   uint64
	Name string
}

func TestLazyFlush(t *testing.T) {
	var entity *lazyReceiverEntity
	var ref *lazyReceiverReference

	registry := &Registry{}
	registry.RegisterEnum("TestEnum", []string{"a", "b", "c"})
	engine := PrepareTables(t, registry, 5, 6, "", entity, ref)
	engine.GetRedis().FlushDB()

	e := &lazyReceiverEntity{Name: "John", Age: 18}
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded := engine.LoadByID(1, e)
	assert.False(t, loaded)

	RunLazyFlushConsumer(engine, false)

	engine.GetLocalCache().Clear()
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)
	assert.Equal(t, uint64(18), e.Age)

	e.Name = "Tom"
	engine.FlushLazy(e)

	e.Age = 30
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)
	assert.Equal(t, uint64(30), e.Age)

	engine.GetLocalCache().Clear()
	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	RunLazyFlushConsumer(engine, false)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	e = &lazyReceiverEntity{}
	e.Name = "Monica"
	e.Age = 18
	engine.Flush(e)

	engine.LoadByID(e.GetID(), e)
	e.Name = "Ivona"
	engine.FlushLazy(e)

	e2 := &lazyReceiverEntity{}
	e2.Name = "Adam"
	e2.Age = 20
	engine.FlushLazy(e2)

	e.Age = 60
	engine.FlushLazy(e)

	RunLazyFlushConsumer(engine, false)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(2, e)
	assert.True(t, loaded)
	assert.Equal(t, "Ivona", e.Name)
	assert.Equal(t, uint64(60), e.Age)

	loaded = engine.LoadByID(3, e2)
	assert.True(t, loaded)
	assert.Equal(t, "Adam", e2.Name)
	assert.Equal(t, uint64(20), e2.Age)

	e1 := &lazyReceiverEntity{}
	e2 = &lazyReceiverEntity{}
	e3 := &lazyReceiverEntity{}
	engine.LoadByID(1, e1)
	engine.LoadByID(2, e2)
	engine.LoadByID(3, e3)
	e1.Name = "Tommy"
	e2.Name = "Tommy2"
	e3.Name = "Tommy3"
	engine.FlushLazy(e1, e2, e3)
	RunLazyFlushConsumer(engine, false)
	e1 = &lazyReceiverEntity{}
	e2 = &lazyReceiverEntity{}
	e3 = &lazyReceiverEntity{}
	engine.LoadByID(1, e1)
	engine.LoadByID(2, e2)
	engine.LoadByID(3, e3)
	assert.Equal(t, "Tommy", e1.Name)
	assert.Equal(t, "Tommy2", e2.Name)
	assert.Equal(t, "Tommy3", e3.Name)

	e = &lazyReceiverEntity{Name: "Tommy2"}
	e.SetOnDuplicateKeyUpdate(Bind{"Age": "38"})
	engine.FlushLazy(e)
	RunLazyFlushConsumer(engine, false)
	engine.LoadByID(2, e)
	assert.Equal(t, uint64(38), e.Age)

	e = &lazyReceiverEntity{Name: "Adam", RefOne: &lazyReceiverReference{Name: "Test"}}
	engine.FlushLazy(e)
	RunLazyFlushConsumer(engine, false)
	engine.LoadByID(5, e)
	assert.Equal(t, "Adam", e.Name)
	assert.Equal(t, uint64(1), e.RefOne.GetID())
	ref = &lazyReceiverReference{}
	assert.True(t, engine.LoadByID(1, ref))
	assert.Equal(t, "Test", ref.Name)

	e = &lazyReceiverEntity{}
	engine.LoadByID(1, e)
	engine.DeleteLazy(e)
	RunLazyFlushConsumer(engine, false)
	loaded = engine.LoadByID(1, e)
	assert.False(t, loaded)

	e = &lazyReceiverEntity{ID: 100}
	engine.Flush(e)
	engine.DeleteLazy(e)
	e = &lazyReceiverEntity{}
	RunLazyFlushConsumer(engine, false)
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.False(t, engine.LoadByID(100, e))
	e2 = &lazyReceiverEntity{}
	e3 = &lazyReceiverEntity{}
	engine.LoadByID(2, e2)
	engine.LoadByID(3, e3)
	e2.Name = "John"
	e3.Name = "Ivona"
	engine.GetMysql().Begin()
	engine.FlushLazy(e2, e3)
	engine.GetMysql().Commit()
	RunLazyFlushConsumer(engine, false)
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	e2 = &lazyReceiverEntity{}
	e3 = &lazyReceiverEntity{}
	engine.LoadByID(2, e2)
	engine.LoadByID(3, e3)
	assert.Equal(t, "John", e2.Name)
	assert.Equal(t, "Ivona", e3.Name)

	e1 = &lazyReceiverEntity{}
	e1.Name = "Ivona"
	e1.Age = 20
	engine.FlushLazy(e1)
	assert.PanicsWithError(t, "Error 1062 (23000): Duplicate entry 'Ivona' for key 'name'", func() {
		RunLazyFlushConsumer(engine, false)
	})
	valid := false

	receiver := NewLazyFlushConsumer(engine)
	receiver.SetBlockTime(0)

	receiver.RegisterLazyFlushQueryErrorResolver(func(engine Engine, event EventEntityFlushed, queryError *mysql.MySQLError) error {
		valid = true
		assert.NotNil(t, e)
		assert.Equal(t, "beeorm.lazyReceiverEntity", event.EntityName())
		assert.Equal(t, Insert, event.Type())
		assert.Len(t, event.Before(), 0)
		assert.Len(t, event.After(), 5)
		assert.Equal(t, "Ivona", event.After()["Name"])
		assert.Error(t, queryError, "Error 1062 (23000): Duplicate entry 'Ivona' for key 'name'")
		return queryError
	})
	assert.PanicsWithError(t, "Error 1062 (23000): Duplicate entry 'Ivona' for key 'name'", func() {
		receiver.Digest(context.Background())
	})
	assert.True(t, valid)
	valid = false
	valid2 := false
	receiver.RegisterLazyFlushQueryErrorResolver(func(engine Engine, event EventEntityFlushed, queryError *mysql.MySQLError) error {
		valid2 = true
		assert.NotNil(t, e)
		assert.Equal(t, "beeorm.lazyReceiverEntity", event.EntityName())
		assert.Error(t, queryError, "Error 1062 (23000): Duplicate entry 'Ivona' for key 'name'")
		return nil
	})
	receiver.Digest(context.Background())
	assert.True(t, valid)
	assert.True(t, valid2)

	e1 = &lazyReceiverEntity{}
	e1.Name = "Adam"
	engine.FlushLazy(e1)
	assert.PanicsWithError(t, "getting ID from lazy flushed entity not allowed", func() {
		e1.GetID()
	})
}
