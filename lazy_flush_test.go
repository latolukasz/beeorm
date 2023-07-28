package beeorm

import (
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
	c := PrepareTables(t, registry, 5, 6, "", entity, ref)
	c.Engine().GetRedis().FlushDB(c)

	e := &lazyReceiverEntity{Name: "John", Age: 18}
	c.Flusher().Track(e).FlushLazy()

	e = GetByID[*lazyReceiverEntity](c, 1)
	assert.Nil(t, e)

	RunLazyFlushConsumer(c, false)

	c.Engine().GetLocalCache().Clear(c)
	e = GetByID[*lazyReceiverEntity](c, 1)
	assert.NotNil(t, e)
	assert.Equal(t, "John", e.Name)
	assert.Equal(t, uint64(18), e.Age)

	e.Name = "Tom"
	c.Flusher().Track(e).FlushLazy()

	e.Age = 30
	c.Flusher().Track(e).FlushLazy()

	e = GetByID[*lazyReceiverEntity](c, 1)
	assert.NotNil(t, e)
	assert.Equal(t, "Tom", e.Name)
	assert.Equal(t, uint64(30), e.Age)

	c.Engine().GetLocalCache().Clear(c)
	e = GetByID[*lazyReceiverEntity](c, 1)
	assert.NotNil(t, e)
	assert.Equal(t, "John", e.Name)

	RunLazyFlushConsumer(c, false)

	e = GetByID[*lazyReceiverEntity](c, 1)
	assert.NotNil(t, e)
	assert.Equal(t, "John", e.Name)

	e = &lazyReceiverEntity{}
	e.Name = "Monica"
	e.Age = 18
	c.Flusher().Track(e).Flush()

	e = GetByID[*lazyReceiverEntity](c, e.ID)
	e.Name = "Ivona"
	c.Flusher().Track(e).FlushLazy()

	e2 := &lazyReceiverEntity{}
	e2.Name = "Adam"
	e2.Age = 20
	c.Flusher().Track(e2).FlushLazy()

	e.Age = 60
	c.Flusher().Track(e).FlushLazy()

	RunLazyFlushConsumer(c, false)

	e = GetByID[*lazyReceiverEntity](c, 2)
	assert.NotNil(t, e)
	assert.Equal(t, "Ivona", e.Name)
	assert.Equal(t, uint64(60), e.Age)

	e = GetByID[*lazyReceiverEntity](c, 3)
	assert.NotNil(t, e)
	assert.Equal(t, "Adam", e2.Name)
	assert.Equal(t, uint64(20), e2.Age)

	e1 := GetByID[*lazyReceiverEntity](c, 1)
	e2 = GetByID[*lazyReceiverEntity](c, 2)
	e3 := GetByID[*lazyReceiverEntity](c, 3)

	e1.Name = "Tommy"
	e2.Name = "Tommy2"
	e3.Name = "Tommy3"
	c.Flusher().Track(e1, e2, e3).FlushLazy()
	RunLazyFlushConsumer(c, false)
	e1 = GetByID[*lazyReceiverEntity](c, 1)
	e2 = GetByID[*lazyReceiverEntity](c, 2)
	e3 = GetByID[*lazyReceiverEntity](c, 3)
	assert.Equal(t, "Tommy", e1.Name)
	assert.Equal(t, "Tommy2", e2.Name)
	assert.Equal(t, "Tommy3", e3.Name)

	e = &lazyReceiverEntity{Name: "Tommy2"}
	e.SetOnDuplicateKeyUpdate(Bind{"Age": 38})
	c.Flusher().Track(e).FlushLazy()
	RunLazyFlushConsumer(c, false)
	e = GetByID[*lazyReceiverEntity](c, 2)
	assert.Equal(t, uint64(38), e.Age)

	e = &lazyReceiverEntity{Name: "Adam", RefOne: &lazyReceiverReference{Name: "Test"}}
	c.Flusher().Track(e).FlushLazy()
	RunLazyFlushConsumer(c, false)
	e = GetByID[*lazyReceiverEntity](c, 5)
	assert.Equal(t, "Adam", e.Name)
	assert.Equal(t, uint64(1), e.RefOne.GetID())
	ref = GetByID[*lazyReceiverReference](c, 1)
	assert.NotNil(t, ref)
	assert.Equal(t, "Test", ref.Name)

	e = GetByID[*lazyReceiverEntity](c, 1)
	c.Flusher().Delete(e).FlushLazy()
	RunLazyFlushConsumer(c, false)
	e = GetByID[*lazyReceiverEntity](c, 1)
	assert.Nil(t, e)

	e = &lazyReceiverEntity{ID: 100}
	c.Flusher().Track(e).Flush()
	c.Flusher().Delete(e).FlushLazy()
	RunLazyFlushConsumer(c, false)
	c.Engine().GetLocalCache().Clear(c)
	c.Engine().GetRedis().FlushDB(c)
	e = GetByID[*lazyReceiverEntity](c, 100)
	assert.Nil(t, e)
	e2 = GetByID[*lazyReceiverEntity](c, 2)
	e3 = GetByID[*lazyReceiverEntity](c, 3)
	e2.Name = "John"
	e3.Name = "Ivona"
	c.Engine().GetMySQL().Begin(c)
	c.Flusher().Track(e2, e3).FlushLazy()
	c.Engine().GetMySQL().Commit(c)
	RunLazyFlushConsumer(c, false)
	c.Engine().GetLocalCache().Clear(c)
	c.Engine().GetRedis().FlushDB(c)
	e2 = GetByID[*lazyReceiverEntity](c, 2)
	e3 = GetByID[*lazyReceiverEntity](c, 3)
	assert.Equal(t, "John", e2.Name)
	assert.Equal(t, "Ivona", e3.Name)

	e1 = &lazyReceiverEntity{}
	e1.Name = "Ivona"
	e1.Age = 20
	c.Flusher().Track(e1).FlushLazy()
	assert.PanicsWithError(t, "Error 1062 (23000): Duplicate entry 'Ivona' for key 'name'", func() {
		RunLazyFlushConsumer(c, false)
	})
	valid := false

	receiver := NewLazyFlushConsumer(c)
	receiver.SetBlockTime(0)

	receiver.RegisterLazyFlushQueryErrorResolver(func(c Context, event EventEntityFlushed, queryError *mysql.MySQLError) error {
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
		receiver.Digest()
	})
	assert.True(t, valid)
	valid = false
	valid2 := false
	receiver.RegisterLazyFlushQueryErrorResolver(func(c Context, event EventEntityFlushed, queryError *mysql.MySQLError) error {
		valid2 = true
		assert.NotNil(t, e)
		assert.Equal(t, "beeorm.lazyReceiverEntity", event.EntityName())
		assert.Error(t, queryError, "Error 1062 (23000): Duplicate entry 'Ivona' for key 'name'")
		return nil
	})
	receiver.Digest()
	assert.True(t, valid)
	assert.True(t, valid2)

	e1 = &lazyReceiverEntity{}
	e1.Name = "Adam"
	c.Flusher().Track(e1).FlushLazy()
	assert.PanicsWithError(t, "getting ID from lazy flushed entity not allowed", func() {
		e1.GetID()
	})
}
