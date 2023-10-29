package beeorm

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type flushEntityAsync struct {
	ID   uint64 `orm:"custom_async_group=test-group"`
	Name string `orm:"required"`
}

type flushEntityAsyncSecondRedis struct {
	ID   uint64 `orm:"custom_async_group=test-group;redisCache=second"`
	Name string `orm:"required"`
}

type flushEntityAsync2 struct {
	ID   uint64 `orm:"custom_async_group=test-group"`
	Name string `orm:"required"`
}

type flushEntityAsync3 struct {
	ID   uint64 `orm:"custom_async_group"`
	Name string `orm:"required"`
}

func TestAsyncConsumer(t *testing.T) {
	registry := NewRegistry()
	c := PrepareTables(t, registry, &flushEntity{}, &flushEntityReference{}, &flushEntityAsync{},
		&flushEntityAsync2{}, &flushEntityAsync3{}, &flushEntityAsyncSecondRedis{})
	schema := getEntitySchema[flushEntity](c)
	schema.DisableCache(true, true)
	schema2 := getEntitySchema[flushEntityReference](c)
	schema2.DisableCache(true, true)
	schema3 := getEntitySchema[flushEntityAsync](c)
	schema4 := getEntitySchema[flushEntityAsync2](c)
	schema5 := getEntitySchema[flushEntityAsyncSecondRedis](c)
	schema6 := getEntitySchema[flushEntityAsync3](c)

	// more than one-page non-blocking mode
	for i := 0; i < asyncConsumerPage+10; i++ {
		reference := NewEntity[flushEntityReference](c)
		reference.Name = "test reference " + strconv.Itoa(i)
	}
	err := c.FlushAsync()
	assert.NoError(t, err)

	err = ConsumeAsyncFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey+flushAsyncEventsListErrorSuffix))

	references := Search[flushEntityReference](c, NewWhere("1"), nil)
	assert.Equal(t, asyncConsumerPage+10, references.Len())
	i := 0
	for references.Next() {
		assert.Equal(t, "test reference "+strconv.Itoa(i), references.Entity().Name)
		i++
	}

	// more than one-page blocking mode
	ctx, cancel := context.WithCancel(context.Background())
	c2 := c.Engine().NewContext(ctx)
	c2.Engine().Registry().(*engineRegistryImplementation).asyncConsumerBlockTime = time.Millisecond * 100

	var consumeErr error
	consumerFinished := false
	go func() {
		consumeErr = ConsumeAsyncFlushEvents(c2, true)
		consumerFinished = true
	}()
	time.Sleep(time.Millisecond * 30)

	reference := NewEntity[flushEntityReference](c)
	reference.Name = "test reference block"
	err = c.FlushAsync()
	assert.NoError(t, err)
	time.Sleep(time.Millisecond * 300)
	cancel()
	time.Sleep(time.Millisecond * 200)
	assert.True(t, consumerFinished)
	assert.NoError(t, consumeErr)
	references = Search[flushEntityReference](c, NewWhere("1"), nil)
	assert.Equal(t, asyncConsumerPage+11, references.Len())
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey+flushAsyncEventsListErrorSuffix))
	assert.Equal(t, "test reference block", GetByID[flushEntityReference](c, reference.ID).Name)

	// custom async group
	reference = NewEntity[flushEntityReference](c)
	reference.Name = "test reference custom async group"
	asyncEntity := NewEntity[flushEntityAsync](c)
	asyncEntity.Name = "test reference custom async group"
	asyncEntity2 := NewEntity[flushEntityAsync2](c)
	asyncEntity2.Name = "test reference custom async group"
	asyncEntity3 := NewEntity[flushEntityAsync3](c)
	asyncEntity3.Name = "test reference custom async group"
	asyncEntitySecondRedis := NewEntity[flushEntityAsyncSecondRedis](c)
	asyncEntitySecondRedis.Name = "test reference custom async group"
	err = c.FlushAsync()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey))
	assert.Equal(t, int64(2), c.Engine().Redis(DefaultPoolCode).LLen(c, schema3.asyncCacheKey))
	assert.Equal(t, int64(2), c.Engine().Redis(DefaultPoolCode).LLen(c, schema4.asyncCacheKey))
	assert.Equal(t, int64(1), c.Engine().Redis("second").LLen(c, schema5.asyncCacheKey))
	assert.Equal(t, int64(1), c.Engine().Redis(DefaultPoolCode).LLen(c, schema6.asyncCacheKey))
	err = ConsumeAsyncFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema3.asyncCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema4.asyncCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis("second").LLen(c, schema5.asyncCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema6.asyncCacheKey))
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityReference](c, reference.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsync](c, asyncEntity.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsync2](c, asyncEntity2.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsync3](c, asyncEntity3.ID).Name)
	assert.Equal(t, "test reference custom async group", GetByID[flushEntityAsyncSecondRedis](c, asyncEntitySecondRedis.ID).Name)

	// broken event structure
	c.Engine().Redis(DefaultPoolCode).RPush(c, schema2.asyncCacheKey, "invalid")
	err = ConsumeAsyncFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.asyncCacheKey))

	// invalid one event, duplicated key
	e1 := NewEntity[flushEntity](c)
	e1.Name = "Valid name 1"
	e1.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e2 := NewEntity[flushEntity](c)
	e2.Name = "Valid name 2"
	e2.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e3 := NewEntity[flushEntity](c)
	e3.Name = "Valid name 3"
	e3.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	err = c.Flush()
	assert.NoError(t, err)
	c.Engine().Redis(DefaultPoolCode).FlushDB(c) // clearing duplicated key data
	e1 = NewEntity[flushEntity](c)
	e1.Name = "Valid name 4"
	e1.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e2 = NewEntity[flushEntity](c)
	e2.Name = "Valid name 2"
	e2.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	e3 = NewEntity[flushEntity](c)
	e3.Name = "Valid name 5"
	e3.ReferenceRequired = &Reference[flushEntityReference]{ID: reference.ID}
	err = c.FlushAsync()
	assert.NoError(t, err)
	err = ConsumeAsyncFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema.asyncCacheKey))
	assert.Equal(t, int64(2), c.Engine().Redis(DefaultPoolCode).LLen(c, schema.asyncCacheKey+flushAsyncEventsListErrorSuffix))
	assert.Contains(t, c.Engine().Redis(DefaultPoolCode).LPop(c, schema.asyncCacheKey+flushAsyncEventsListErrorSuffix), "INSERT INTO `flushEntity`")
	assert.Equal(t, "Error 1062 (23000): Duplicate entry 'Valid name 2' for key 'flushEntity.name'", c.Engine().Redis(DefaultPoolCode).LPop(c, schema.asyncCacheKey+flushAsyncEventsListErrorSuffix))
}
