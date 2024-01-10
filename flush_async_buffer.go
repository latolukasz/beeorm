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

func ConsumeAsyncBuffer(orm ORM, errF func(err error)) (stop func()) {
	engine := orm.Engine().(*engineImplementation)
	if engine.asyncTemporaryIsQueueRunning {
		panic("consumer is already running")
	}
	engine.asyncTemporaryIsQueueRunning = true
	schemas := orm.Engine().Registry().Entities()
	stop = func() {
		if !engine.asyncTemporaryIsQueueRunning {
			return
		}
		for _, schema := range schemas {
			schema.(*entitySchema).asyncTemporaryQueue.TryEnqueue(nil)
		}
		maxIterations := 10000
		for {
			maxIterations--
			if maxIterations == 0 {
				return
			}
			if engine.asyncTemporaryIsQueueRunning {
				time.Sleep(time.Millisecond)
				continue
			}
			return
		}
	}
	go func() {
		waitGroup := &sync.WaitGroup{}
		for _, schema := range schemas {
			var schemaLocal = schema.(*entitySchema)
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				consumeAsyncTempEvent(orm.Clone(), schemaLocal, errF)
			}()
		}
		waitGroup.Wait()
		engine.asyncTemporaryIsQueueRunning = false
	}()
	return stop
}

func consumeAsyncTempEvent(orm ORM, schema *entitySchema, errF func(err error)) {
	r := orm.Engine().Redis(schema.getForcedRedisCode())
	buffer := make([]any, redisRPushPackSize)
	var values []any
	var ok bool
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
			for {
				values, ok = schema.asyncTemporaryQueue.TryDequeue()
				if !ok {
					time.Sleep(time.Millisecond * 200)
					continue
				}
				break
			}

			if values == nil {
				return false
			}
			rows := 1
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(values)
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
			r.RPush(orm, schema.asyncCacheKey, buffer[0:rows]...)
			return !breakMe
		}()
		if !res {
			return
		}
	}
}
