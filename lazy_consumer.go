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
	groups := make(map[DB]map[RedisCache]map[string]bool)
	for _, entityType := range c.Engine().Registry().Entities() {
		schema := c.Engine().Registry().EntitySchema(entityType).(*entitySchema)
		db := schema.GetDB()
		dbGroup, has := groups[db]
		if !has {
			dbGroup = make(map[RedisCache]map[string]bool)
			groups[db] = dbGroup
		}
		r := c.Engine().Redis(schema.getLazyRedisCode())
		redisGroup, has := dbGroup[r]
		if !has {
			redisGroup = make(map[string]bool)
			dbGroup[r] = redisGroup
		}
		_, has = redisGroup[schema.lazyCacheKey]
		if has {
			continue
		}
		redisGroup[schema.lazyCacheKey] = true
		waitGroup.Add(1)
		go consumeLazyEvents(ctxNoCancel.Clone(), c.Ctx(), schema.lazyCacheKey, db, r, block, waitGroup)
	}
	waitGroup.Wait()
	return nil
}

func consumeLazyEvents(c Context, ctx context.Context, list string, db DB, r RedisCache, block bool, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	var values []string
	for {
		if ctx.Err() != nil {
			return
		}
		values = r.LRange(c, list, 0, lazyConsumerPage-1)
		if len(values) > 0 {
			handleLazyEvents(c, ctx, list, db, r, values)
		}
		if len(values) < lazyConsumerPage {
			if !block || ctx.Err() != nil {
				return
			}
			time.Sleep(c.Engine().Registry().(*engineRegistryImplementation).lazyConsumerBlockTime)
		}
	}
}

func handleLazyEvents(c Context, ctx context.Context, list string, db DB, r RedisCache, values []string) {
	operations := len(values)
	inTX := operations > 1
	func() {
		var d DBBase
		defer func() {
			if inTX {
				d.(DBTransaction).Rollback(c)
			}
		}()
		dbPool := db
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
				handleLazyEventsOneByOne(c, ctx, list, db, r, values)
				return
			}
		}
		if inTX {
			d.(DBTransaction).Commit(c)
		}
		r.Ltrim(c, list, int64(len(values)), -1)
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

func handleLazyEventsOneByOne(c Context, ctx context.Context, list string, db DB, r RedisCache, values []string) {
	for _, event := range values {
		if ctx.Err() != nil {
			return
		}
		err := handleLazyEvent(c, db, event)
		if err != nil {
			r.RPush(c, list+flushLazyEventsListErrorSuffix, event, err.Error())
		}
		r.Ltrim(c, list, 1, -1)
	}
}