package beeorm

import (
	"context"
	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"sync"
	"time"
)

const lazyConsumerPage = 1000
const lazyConsumerBlockTime = time.Second * 3

func ConsumeLazyFlushEvents(c Context, block bool) error {
	waitGroup := &sync.WaitGroup{}
	ctxNoCancel := c.CloneWithContext(context.Background())
	for _, entityType := range c.Engine().Registry().Entities() {
		waitGroup.Add(1)
		go consumeLazyEvents(ctxNoCancel.Clone(), c.Ctx(), c.Engine().Registry().EntitySchema(entityType).(*entitySchema), block, waitGroup)
	}
	waitGroup.Wait()
	return nil
}

func consumeLazyEvents(c Context, ctx context.Context, schema *entitySchema, block bool, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	r := c.Engine().Redis(schema.getLazyRedisCode())
	var values []string
	for {
		if ctx.Err() != nil {
			return
		}
		values = r.LRange(c, schema.lazyCacheKey, 0, lazyConsumerPage-1)
		if len(values) > 0 {
			handleLazyEvents(c, ctx, schema, values)
		}
		if len(values) < lazyConsumerPage {
			if !block || ctx.Err() != nil {
				return
			}
			time.Sleep(c.Engine().Registry().(*engineRegistryImplementation).lazyConsumerBlockTime)
		}
	}
}

func handleLazyEvents(c Context, ctx context.Context, schema *entitySchema, values []string) {
	operations := len(values)
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
		for _, event := range values {
			if ctx.Err() != nil {
				return
			}
			err := handleLazyEvent(c, d, event)
			if err != nil {
				if inTX {
					d.(DBTransaction).Rollback(c)
				}
				handleLazyEventsOneByOne(c, ctx, schema, values)
				return
			}
		}
		if inTX {
			d.(DBTransaction).Commit(c)
		}
		c.Engine().Redis(schema.getLazyRedisCode()).Ltrim(c, schema.lazyCacheKey, int64(len(values)), -1)
	}()
}

func handleLazyEvent(c Context, db DBBase, value string) (err *mysql.MySQLError) {
	defer func() {
		if rec := recover(); rec != nil {
			asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
			if isMySQLError {
				// 1062 - Duplicate entry
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

func handleLazyEventsOneByOne(c Context, ctx context.Context, schema *entitySchema, values []string) {
	r := c.Engine().Redis(schema.getLazyRedisCode())
	dbPool := schema.GetDB()
	for _, event := range values {
		if ctx.Err() != nil {
			return
		}
		err := handleLazyEvent(c, dbPool, event)
		if err != nil {
			r.RPush(c, schema.lazyCacheKey+":err", event, err.Error())
		}
		r.Ltrim(c, schema.lazyCacheKey, 1, -1)
	}
}
