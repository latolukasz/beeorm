package beeorm

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestLazyConsumer(t *testing.T) {
	registry := &Registry{}
	c := PrepareTables(t, registry, "", &flushEntity{}, &flushEntityReference{})
	schema := GetEntitySchema[*flushEntity](c)
	schema.DisableCache(true, true)

	ctx4, cancel4 := context.WithCancel(context.Background())
	c4 := c.Engine().NewContext(ctx4)
	rr := c4.Engine().Redis(DefaultPoolCode)
	go func() {
		for {
			select {
			case <-c4.Ctx().Done():
				return
			default:
				fmt.Printf("A1\n")
				rr.BLMove(c4, "AAA", "BBB", "LEFT", "RIGHT", 0)
				fmt.Printf("A2\n")
			}
		}

	}()
	time.Sleep(time.Second)
	cancel4()
	time.Sleep(time.Second)
	os.Exit(1)

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

	var consumeErr error
	consumerFinished := false
	fmt.Printf("STARTED\n\n\n")
	go func() {
		consumeErr = ConsumeLazyFlushEvents(c2, true)
		consumerFinished = true
	}()
	time.Sleep(time.Millisecond * 300)

	reference := NewEntity[*flushEntityReference](c).TrackedEntity()
	reference.Name = "test reference block"
	err = c.Flush(true)
	assert.NoError(t, err)
	cancel()
	time.Sleep(time.Millisecond * 3000)
	assert.True(t, consumerFinished)
	assert.NoError(t, consumeErr)
}
