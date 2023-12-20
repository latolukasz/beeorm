package beeorm

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type flushEntityAsync struct {
	ID   uint64
	Name string `orm:"required"`
}

type flushEntityAsyncSecondRedis struct {
	ID   uint64 `orm:"redisCache=second"`
	Name string `orm:"required"`
}

type flushEntityAsync2 struct {
	ID   uint64
	Name string `orm:"required"`
}

type flushEntityAsync3 struct {
	ID   uint64 `orm:"split_async_flush"`
	Name string `orm:"required"`
}

func TestAsyncConsumer(t *testing.T) {
	registry := NewRegistry()
	orm := PrepareTables(t, registry, flushEntity{}, flushEntityReference{}, flushEntityAsync{},
		flushEntityAsync2{}, flushEntityAsync3{}, flushEntityAsyncSecondRedis{})
	schema := getEntitySchema[flushEntity](orm)
	schema.DisableCache(true, true)
	schema2 := getEntitySchema[flushEntityReference](orm)
	schema2.DisableCache(true, true)
	schema3 := getEntitySchema[flushEntityAsync](orm)
	schema4 := getEntitySchema[flushEntityAsync2](orm)
	schema5 := getEntitySchema[flushEntityAsyncSecondRedis](orm)
	schema6 := getEntitySchema[flushEntityAsync3](orm)

	// more than one-page non-blocking mode
	for i := 0; i < asyncConsumerPage+10; i++ {
		reference := NewEntity[flushEntityReference](orm)
		reference.Name = "test reference " + strconv.Itoa(i)
	}
	err := orm.FlushAsync()
	assert.NoError(t, err)

	err = runAsyncConsumer(orm, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey))
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey+flushAsyncEventsListErrorSuffix))

	references := Search[flushEntityReference](orm, NewWhere("1"), nil)
	assert.Equal(t, asyncConsumerPage+10, references.Len())
	i := 0
	for references.Next() {
		assert.Equal(t, "test reference "+strconv.Itoa(i), references.Entity().Name)
		i++
	}

	// more than one-page blocking mode
	ctx, cancel := context.WithCancel(context.Background())
	c2 := orm.Engine().NewORM(ctx)
	c2.Engine().Registry().(*engineRegistryImplementation).asyncConsumerBlockTime = time.Millisecond * 100

	var consumeErr error
	consumerFinished := false
	go func() {
		stop := ConsumeAsyncBuffer(c2, func(err error) {
			panic(err)
		})
		stop()
		consumeErr = ConsumeAsyncFlushEvents(c2, true)
		consumerFinished = true
	}()
	time.Sleep(time.Millisecond * 30)

	reference := NewEntity[flushEntityReference](orm)
	reference.Name = "test reference block"
	err = orm.FlushAsync()
	assert.NoError(t, err)
	stop := ConsumeAsyncBuffer(c2, func(err error) {
		panic(err)
	})
	stop()
	time.Sleep(time.Millisecond * 300)
	cancel()
	time.Sleep(time.Millisecond * 200)
	assert.True(t, consumerFinished)
	assert.NoError(t, consumeErr)
	references = Search[flushEntityReference](orm, NewWhere("1"), nil)
	assert.Equal(t, asyncConsumerPage+11, references.Len())
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey))
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey+flushAsyncEventsListErrorSuffix))
	assert.Equal(t, "test reference block", GetByID[flushEntityReference](orm, reference.ID).Name)

	// custom async group
	reference = NewEntity[flushEntityReference](orm)
	reference.Name = "test reference custom async group"
	asyncEntity := NewEntity[flushEntityAsync](orm)
	asyncEntity.Name = "test reference custom async group"
	asyncEntity2 := NewEntity[flushEntityAsync2](orm)
	asyncEntity2.Name = "test reference custom async group"
	asyncEntity3 := NewEntity[flushEntityAsync3](orm)
	asyncEntity3.Name = "test reference custom async group"
	asyncEntitySecondRedis := NewEntity[flushEntityAsyncSecondRedis](orm)
	asyncEntitySecondRedis.Name = "test reference custom async group"
	err = orm.FlushAsync()
	assert.NoError(t, err)
	stop = ConsumeAsyncBuffer(orm, func(err error) {
		panic(err)
	})
	stop()
	time.Sleep(time.Second)
	assert.Equal(t, int64(3), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey))
	assert.Equal(t, int64(1), orm.Engine().Redis("second").LLen(orm, schema5.asyncCacheKey))
	assert.Equal(t, int64(1), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema6.asyncCacheKey))
	err = runAsyncConsumer(orm, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey))
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema3.asyncCacheKey))
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema4.asyncCacheKey))
	assert.Equal(t, int64(0), orm.Engine().Redis("second").LLen(orm, schema5.asyncCacheKey))
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema6.asyncCacheKey))
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityReference](orm, reference.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsync](orm, asyncEntity.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsync2](orm, asyncEntity2.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsync3](orm, asyncEntity3.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsyncSecondRedis](orm, asyncEntitySecondRedis.ID).Name)

	// broken event structure
	orm.Engine().Redis(DefaultPoolCode).RPush(orm, schema2.asyncCacheKey, "invalid")
	err = runAsyncConsumer(orm, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema2.asyncCacheKey))

	// invalid one event, duplicated key
	e1 := NewEntity[flushEntity](orm)
	e1.Name = "Valid name 1"
	e1.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e2 := NewEntity[flushEntity](orm)
	e2.Name = "Valid name 2"
	e2.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e3 := NewEntity[flushEntity](orm)
	e3.Name = "Valid name 3"
	e3.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	err = orm.Flush()
	assert.NoError(t, err)
	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm) // clearing duplicated key data
	e1 = NewEntity[flushEntity](orm)
	e1.Name = "Valid name 4"
	e1.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e2 = NewEntity[flushEntity](orm)
	e2.Name = "Valid name 2"
	e2.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e3 = NewEntity[flushEntity](orm)
	e3.Name = "Valid name 5"
	e3.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	err = orm.FlushAsync()
	assert.NoError(t, err)
	err = runAsyncConsumer(orm, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema.asyncCacheKey))
	assert.Equal(t, int64(2), orm.Engine().Redis(DefaultPoolCode).LLen(orm, schema.asyncCacheKey+flushAsyncEventsListErrorSuffix))
	assert.Contains(t, orm.Engine().Redis(DefaultPoolCode).LPop(orm, schema.asyncCacheKey+flushAsyncEventsListErrorSuffix), "INSERT INTO `flushEntity`")
	assert.Equal(t, "Error 1062 (23000): Duplicate entry 'Valid name 2' for key 'flushEntity.name'", orm.Engine().Redis(DefaultPoolCode).LPop(orm, schema.asyncCacheKey+flushAsyncEventsListErrorSuffix))
}

func runAsyncConsumer(orm ORM, block bool) error {
	stop := ConsumeAsyncBuffer(orm, func(err error) {
		panic(err)
	})
	stop()
	return ConsumeAsyncFlushEvents(orm, block)
}
