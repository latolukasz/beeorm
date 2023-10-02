package beeorm

import (
	"context"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestLazyConsumer(t *testing.T) {
	registry := &Registry{}
	c := PrepareTables(t, registry, "", &flushEntity{}, &flushEntityReference{})
	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(true, true)

	// more than one-page non-blocking mode
	for i := 0; i < lazyConsumerPage+10; i++ {
		reference := NewEntity[*flushEntityReference](c).TrackedEntity()
		reference.Name = "test reference " + strconv.Itoa(i)
	}
	err := c.Flush(true)
	assert.NoError(t, err)
	err = ConsumeLazyFlushEvents(c, false)
	assert.Nil(t, err)

	references := Search[*flushEntityReference](c, NewWhere("1"), nil)
	assert.Len(t, references, lazyConsumerPage+10)
	for i := 0; i < lazyConsumerPage+10; i++ {
		assert.Equal(t, "test reference "+strconv.Itoa(i), references[i].Name)
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

	reference := NewEntity[*flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference block"
	err = c.Flush(true)
	assert.NoError(t, err)
	time.Sleep(time.Millisecond * 300)
	cancel()
	time.Sleep(time.Millisecond * 200)
	assert.True(t, consumerFinished)
	assert.NoError(t, consumeErr)
	references = Search[*flushEntityReference](c, NewWhere("1"), nil)
	assert.Len(t, references, lazyConsumerPage+11)
}
