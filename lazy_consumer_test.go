package beeorm

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type flushEntityLazy struct {
	ID   uint64 `orm:"custom_lazy_group=test-group"`
	Name string `orm:"required"`
}

type flushEntityLazySecondRedis struct {
	ID   uint64 `orm:"custom_lazy_group=test-group;redisCache=second"`
	Name string `orm:"required"`
}

type flushEntityLazy2 struct {
	ID   uint64 `orm:"custom_lazy_group=test-group"`
	Name string `orm:"required"`
}

type flushEntityLazy3 struct {
	ID   uint64 `orm:"custom_lazy_group"`
	Name string `orm:"required"`
}

func TestLazyConsumer(t *testing.T) {
	registry := &Registry{}
	c := PrepareTables(t, registry, &flushEntity{}, &flushEntityReference{}, &flushEntityLazy{},
		&flushEntityLazy2{}, &flushEntityLazy3{}, &flushEntityLazySecondRedis{})
	schema := getEntitySchema[flushEntity](c)
	schema.DisableCache(true, true)
	schema2 := getEntitySchema[flushEntityReference](c)
	schema2.DisableCache(true, true)
	schema3 := getEntitySchema[flushEntityLazy](c)
	schema4 := getEntitySchema[flushEntityLazy2](c)
	schema5 := getEntitySchema[flushEntityLazySecondRedis](c)
	schema6 := getEntitySchema[flushEntityLazy3](c)

	// more than one-page non-blocking mode
	for i := 0; i < lazyConsumerPage+10; i++ {
		reference := NewEntity[flushEntityReference](c).TrackedEntity()
		reference.Name = "test reference " + strconv.Itoa(i)
	}
	err := c.Flush(true)
	assert.NoError(t, err)

	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey+flushLazyEventsListErrorSuffix))

	references := Search[flushEntityReference](c, NewWhere("1"), nil)
	assert.Equal(t, lazyConsumerPage+10, references.Len())
	i := 0
	for references.Next() {
		assert.Equal(t, "test reference "+strconv.Itoa(i), references.Entity().Name)
		i++
	}

	// more than one-page blocking mode
	ctx, cancel := context.WithCancel(context.Background())
	c2 := c.Engine().NewContext(ctx)
	c2.Engine().Registry().(*engineRegistryImplementation).lazyConsumerBlockTime = time.Millisecond * 100

	var consumeErr error
	consumerFinished := false
	go func() {
		consumeErr = ConsumeLazyFlushEvents(c2, true)
		consumerFinished = true
	}()
	time.Sleep(time.Millisecond * 30)

	reference := NewEntity[flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference block"
	err = c.Flush(true)
	assert.NoError(t, err)
	time.Sleep(time.Millisecond * 300)
	cancel()
	time.Sleep(time.Millisecond * 200)
	assert.True(t, consumerFinished)
	assert.NoError(t, consumeErr)
	references = Search[flushEntityReference](c, NewWhere("1"), nil)
	assert.Equal(t, lazyConsumerPage+11, references.Len())
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey+flushLazyEventsListErrorSuffix))
	assert.Equal(t, "test reference block", GetByID[flushEntityReference](c, reference.ID).Name)

	// custom lazy group
	reference = NewEntity[flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference custom lazy group"
	lazyEntity := NewEntity[flushEntityLazy](c).TrackedEntity()
	lazyEntity.Name = "test reference custom lazy group"
	lazyEntity2 := NewEntity[flushEntityLazy2](c).TrackedEntity()
	lazyEntity2.Name = "test reference custom lazy group"
	lazyEntity3 := NewEntity[flushEntityLazy3](c).TrackedEntity()
	lazyEntity3.Name = "test reference custom lazy group"
	lazyEntitySecondRedis := NewEntity[flushEntityLazySecondRedis](c).TrackedEntity()
	lazyEntitySecondRedis.Name = "test reference custom lazy group"
	err = c.Flush(true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey))
	assert.Equal(t, int64(2), c.Engine().Redis(DefaultPoolCode).LLen(c, schema3.lazyCacheKey))
	assert.Equal(t, int64(2), c.Engine().Redis(DefaultPoolCode).LLen(c, schema4.lazyCacheKey))
	assert.Equal(t, int64(1), c.Engine().Redis("second").LLen(c, schema5.lazyCacheKey))
	assert.Equal(t, int64(1), c.Engine().Redis(DefaultPoolCode).LLen(c, schema6.lazyCacheKey))
	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema3.lazyCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema4.lazyCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis("second").LLen(c, schema5.lazyCacheKey))
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema6.lazyCacheKey))
	assert.Equal(t, "test reference custom lazy group", GetByID[flushEntityReference](c, reference.ID).Name)
	assert.Equal(t, "test reference custom lazy group", GetByID[flushEntityLazy](c, lazyEntity.ID).Name)
	assert.Equal(t, "test reference custom lazy group", GetByID[flushEntityLazy2](c, lazyEntity2.ID).Name)
	assert.Equal(t, "test reference custom lazy group", GetByID[flushEntityLazy3](c, lazyEntity3.ID).Name)
	assert.Equal(t, "test reference custom lazy group", GetByID[flushEntityLazySecondRedis](c, lazyEntitySecondRedis.ID).Name)

	// broken event structure
	c.Engine().Redis(DefaultPoolCode).RPush(c, schema2.lazyCacheKey, "invalid")
	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema2.lazyCacheKey))

	// invalid one event, duplicated key
	e1 := NewEntity[flushEntity](c).TrackedEntity()
	e1.Name = "Valid name 1"
	e1.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	e2 := NewEntity[flushEntity](c).TrackedEntity()
	e2.Name = "Valid name 2"
	e2.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	e3 := NewEntity[flushEntity](c).TrackedEntity()
	e3.Name = "Valid name 3"
	e3.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(false)
	assert.NoError(t, err)
	c.Engine().Redis(DefaultPoolCode).FlushDB(c) // clearing duplicated key data
	e1 = NewEntity[flushEntity](c).TrackedEntity()
	e1.Name = "Valid name 4"
	e1.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	e2 = NewEntity[flushEntity](c).TrackedEntity()
	e2.Name = "Valid name 2"
	e2.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	e3 = NewEntity[flushEntity](c).TrackedEntity()
	e3.Name = "Valid name 5"
	e3.ReferenceRequired = NewReference[flushEntityReference](reference.ID)
	err = c.Flush(true)
	assert.NoError(t, err)
	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), c.Engine().Redis(DefaultPoolCode).LLen(c, schema.lazyCacheKey))
	assert.Equal(t, int64(2), c.Engine().Redis(DefaultPoolCode).LLen(c, schema.lazyCacheKey+flushLazyEventsListErrorSuffix))
	assert.Contains(t, c.Engine().Redis(DefaultPoolCode).LPop(c, schema.lazyCacheKey+flushLazyEventsListErrorSuffix), "INSERT INTO `flushEntity`")
	assert.Equal(t, "Error 1062 (23000): Duplicate entry 'Valid name 2' for key 'flushEntity.name'", c.Engine().Redis(DefaultPoolCode).LPop(c, schema.lazyCacheKey+flushLazyEventsListErrorSuffix))
}
