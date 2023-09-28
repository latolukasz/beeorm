package beeorm

import (
	"github.com/go-sql-driver/mysql"
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
	operations := len(values)
	if tmpValue != "" {
		operations++
	}
	inTX := operations > 1
	func() {
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
			err := handleLazyEvent(c, d, tmpValue)
			if err != nil {
				if inTX {
					d.(DBTransaction).Rollback(c)
				}
				handleLazyEventsOneByOne(c, schema, tmpValue, values)
				return
			}
		}
		for _, event := range values {
			err := handleLazyEvent(c, d, event)
			if err != nil {
				if inTX {
					d.(DBTransaction).Rollback(c)
				}
				handleLazyEventsOneByOne(c, schema, tmpValue, values)
				return
			}
		}
		if inTX {
			d.(DBTransaction).Commit(c)
		}
	}()
}

func handleLazyEvent(c Context, db DBBase, value string) (err *mysql.MySQLError) {
	defer func() {
		if rec := recover(); rec != nil {
			asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
			if isMySQLError {
				err = asMySQLError
				// return only if strange sql errors
				return
			}
			panic(rec)
		}
	}()
	var data []interface{}
	_ = jsoniter.ConfigFastest.UnmarshalFromString(value, &data)
	if len(data) == 0 {
		return nil
	}
	sql, valid := data[0].(string)
	if !valid {
		return
	}
	if len(data) == 1 {
		db.Exec(c, sql)
		return nil
	}
	for i, arg := range data[1:] {
		if arg == nullAsString {
			data[i+1] = nil
		}
	}
	db.Exec(c, sql, data[1:]...)
	return nil
}

func handleLazyEventsOneByOne(c Context, schema *entitySchema, tmpValue string, values []string) {
	r := c.Engine().Redis(schema.getLazyRedisCode())
	dbPool := schema.GetDB()
	if tmpValue != "" {
		err := handleLazyEvent(c, dbPool, tmpValue)
		if err != nil {
			r.RPush(c, schema.lazyCacheKey+":err", tmpValue)
			r.RPush(c, schema.lazyCacheKey+":err", err)
		}
		r.Ltrim(c, schema.lazyCacheKey+":tmp", 0, 0)
	}
	for _, event := range values {
		err := handleLazyEvent(c, dbPool, event)
		if err != nil {
			r.RPush(c, schema.lazyCacheKey+":err", event)
			r.RPush(c, schema.lazyCacheKey+":err", err)
		}
		r.Ltrim(c, schema.lazyCacheKey, 0, 0)
	}
}
