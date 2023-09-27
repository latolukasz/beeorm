package beeorm

import (
	"fmt"
	"sync"
	"time"
)

const lazyConsumerPage = 1000
const lazyConsumerWaitTime = time.Second

func ConsumeLazyFlushEvents(c Context, block bool) error {
	waitGroup := &sync.WaitGroup{}
	for _, entityType := range c.Engine().Registry().Entities() {
		go consumeLazyEvents(c.Clone(), c.Engine().Registry().EntitySchema(entityType).(*entitySchema), block, waitGroup)
	}
	waitGroup.Wait()
	fmt.Printf("DONE\n")
	return nil
}

func consumeLazyEvents(c Context, schema *entitySchema, block bool, waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	fmt.Printf("%s %s\n", schema.GetTableName(), schema.lazyCacheKey)
	defer waitGroup.Done()
	r := c.Engine().Redis(schema.getLazyRedisCode())
	tmpList := schema.lazyCacheKey + ":tmp"
	clearTemp := true
	var values []string
	for {
		source := schema.lazyCacheKey
		if clearTemp {
			source = tmpList
		}
		values = r.LRange(c, source, 0, lazyConsumerPage-1)
		if len(values) > 0 {
			handleLazyEvents(c, schema, "", values)
			r.Ltrim(c, tmpList, 0, int64(len(values)-1))
		}
		if len(values) < lazyConsumerPage {
			source = schema.lazyCacheKey
			clearTemp = false
			if !block {
				return
			}
			tmp := r.BLMove(c, schema.lazyCacheKey, tmpList, "LEFT", "RIGHT", 0)
			values = r.LRange(c, schema.lazyCacheKey, 0, lazyConsumerPage-1)
			handleLazyEvents(c, schema, tmp, values)
			r.Ltrim(c, tmpList, -1, -1)
			if len(values) > 0 {
				r.Ltrim(c, schema.lazyCacheKey, 0, int64(len(values)-1))
			}
		}
	}
}

func handleLazyEvents(c Context, schema *entitySchema, tmpValue string, values []string) {

}
