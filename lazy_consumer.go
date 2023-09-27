package beeorm

import (
	jsoniter "github.com/json-iterator/go"
	"sync"
)

const lazyConsumerPage = 1000

func ConsumeLazyFlushEvents(c Context, block bool) error {
	waitGroup := &sync.WaitGroup{}
	for _, entityType := range c.Engine().Registry().Entities() {
		waitGroup.Add(1)
		go consumeLazyEvents(c.Clone(), c.Engine().Registry().EntitySchema(entityType).(*entitySchema), block, waitGroup)
	}
	waitGroup.Wait()
	return nil
}

func consumeLazyEvents(c Context, schema *entitySchema, block bool, waitGroup *sync.WaitGroup) {
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
			var tmp = ""
			if block {
				tmp = r.BLMove(c, schema.lazyCacheKey, tmpList, "LEFT", "RIGHT", 0)
			}
			values = r.LRange(c, schema.lazyCacheKey, 0, lazyConsumerPage-1)
			handleLazyEvents(c, schema, tmp, values)
			if tmp != "" {
				r.Ltrim(c, tmpList, -1, -1)
			}
			if len(values) > 0 {
				r.Ltrim(c, schema.lazyCacheKey, 0, int64(len(values)-1))
			}
			if !block {
				return
			}
		}
	}
}

func handleLazyEvents(c Context, schema *entitySchema, tmpValue string, values []string) {
	if tmpValue == "" && len(values) == 0 {
		return
	}
	inTX := len(values) > 2
	var d DBBase
	defer func() {
		if inTX {
			d.(DBTransaction).Rollback(c)
		}
	}()
	dbPool := schema.GetDB()
	if inTX {
		d = dbPool.Begin(c)
	} else {
		d = dbPool
	}
	if tmpValue != "" {
		handleLazyEvent(c, d, tmpValue)
	}
	for _, event := range values {
		handleLazyEvent(c, d, event)
	}
	if inTX {
		d.(DBTransaction).Commit(c)
	}
}

func handleLazyEvent(c Context, db DBBase, value string) {
	var data []interface{}
	_ = jsoniter.ConfigFastest.UnmarshalFromString(value, &data)
	if len(data) == 0 {
		return
	}
	sql, valid := data[0].(string)
	if !valid {
		return
	}
	if len(data) == 1 {
		db.Exec(c, sql)
		return
	}
	db.Exec(c, sql, data[1:]...)
}
