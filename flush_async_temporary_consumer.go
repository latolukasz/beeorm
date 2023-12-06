package beeorm

import (
	"fmt"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const redisRPushPackSize = 1000

type asyncTemporaryQueueEvent []any

func publishAsyncEvent(schema *entitySchema, event asyncTemporaryQueueEvent) {
	schema.asyncTemporaryQueue.Enqueue(event)
}

func ConsumeAsyncFlushTemporaryEvents(c Context, errF func(err error)) (stop func()) {
	entities := c.Engine().Registry().Entities()
	running := true
	stop = func() {
		if !running {
			return
		}
		for _, entityType := range entities {
			schema := c.Engine().Registry().EntitySchema(entityType).(*entitySchema)
			schema.asyncTemporaryQueue.TryEnqueue(nil)
		}
		maxIterations := 5000
		for {
			if running {
				time.Sleep(time.Millisecond * 10)
				return
			}
			maxIterations--
			if maxIterations == 0 {
				return
			}
			return
		}
	}
	go func() {
		waitGroup := &sync.WaitGroup{}
		for _, entityType := range entities {
			schema := c.Engine().Registry().EntitySchema(entityType).(*entitySchema)
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				consumeAsyncTempEvent(c.Clone(), schema, errF)
			}()
		}
		go func() {
			<-c.Ctx().Done()
			stop()
		}()
		waitGroup.Wait()
		running = false
	}()
	return stop
}

func consumeAsyncTempEvent(c Context, schema *entitySchema, errF func(err error)) {
	r := c.Engine().Redis(schema.getForcedRedisCode())
	buffer := make([]any, redisRPushPackSize)
	for {
		res := func() bool {
			defer func() {
				if rec := recover(); rec != nil {
					asError, isError := rec.(error)
					if !isError {
						asError = fmt.Errorf("%v", rec)
					}
					errF(asError)
					time.Sleep(time.Second * 3)
				}
			}()
			e := schema.asyncTemporaryQueue.Dequeue()
			if e == nil {
				return false
			}
			rows := 1
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(e)
			buffer[0] = asJSON
			breakMe := false
			for i := 1; i < redisRPushPackSize; i++ {
				e, has := schema.asyncTemporaryQueue.TryDequeue()
				if !has {
					break
				}
				if e == nil {
					breakMe = true
					break
				}
				asJSON, _ = jsoniter.ConfigFastest.MarshalToString(e)
				buffer[i] = asJSON
				rows++
			}
			r.RPush(c, schema.asyncCacheKey, buffer[0:rows]...)
			return !breakMe
		}()
		if !res {
			return
		}
	}
}
