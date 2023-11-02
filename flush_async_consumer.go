package beeorm

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
)

const asyncConsumerPage = 1000
const asyncConsumerBlockTime = time.Second * 3
const flushAsyncEventsList = "flush_async_events"
const flushAsyncEventsListErrorSuffix = ":err"

var mySQLErrorCodesToSkip = []uint16{
	1022, // Can't write; duplicate key in table '%s'
	1048, // Column '%s' cannot be null
	1049, // Unknown database '%s'
	1051, // Unknown table '%s'
	1054, // Unknown column '%s' in '%s'
	1062, // Duplicate entry '%s' for key %d
	1063, // Incorrect column specifier for column '%s'
	1064, // Syntax error
	1067, // Invalid default value for '%s'
	1109, // Message: Unknown table '%s' in %s
	1146, // Table '%s.%s' doesn't exist
	1149, // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use
	2032, // Data truncated
}

const asyncConsumerLockName = "async_consumer"

func ConsumeAsyncFlushEvents(c Context, block bool) error {
	lock, lockObtained := c.Engine().Redis(DefaultPoolCode).GetLocker().Obtain(c, asyncConsumerLockName, time.Minute, 0)
	if !lockObtained {
		return nil
	}
	defer func() {
		lock.Release(c)
	}()
	go func() {
		time.Sleep(time.Second * 50)
		if !lock.Refresh(c, time.Minute) {
			lockObtained = false
		}
	}()
	errorMutex := sync.Mutex{}
	waitGroup := &sync.WaitGroup{}
	ctxNoCancel := c.CloneWithContext(context.Background())
	groups := make(map[DB]map[RedisCache]map[string]bool)
	var stop uint32
	var globalError error
	for _, entityType := range c.Engine().Registry().Entities() {
		schema := c.Engine().Registry().EntitySchema(entityType).(*entitySchema)
		db := schema.GetDB()
		dbGroup, has := groups[db]
		if !has {
			dbGroup = make(map[RedisCache]map[string]bool)
			groups[db] = dbGroup
		}
		r := c.Engine().Redis(schema.getForcedRedisCode())
		redisGroup, has := dbGroup[r]
		if !has {
			redisGroup = make(map[string]bool)
			dbGroup[r] = redisGroup
		}
		_, has = redisGroup[schema.asyncCacheKey]
		if has {
			continue
		}
		redisGroup[schema.asyncCacheKey] = true
		waitGroup.Add(1)
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					atomic.AddUint32(&stop, 1)
					asError, isError := rec.(error)
					if !isError {
						asError = fmt.Errorf("%v", rec)
					}
					if globalError == nil {
						errorMutex.Lock()
						globalError = asError
						errorMutex.Unlock()
					}
				}
			}()
			consumeAsyncEvents(c.Ctx(), ctxNoCancel.Clone(), schema.asyncCacheKey, db, r, block, waitGroup, &lockObtained, &stop)
		}()
	}
	waitGroup.Wait()
	return globalError
}

func consumeAsyncEvents(ctx context.Context, c Context, list string, db DB, r RedisCache,
	block bool, waitGroup *sync.WaitGroup, lockObtained *bool, stop *uint32) {
	defer waitGroup.Done()
	var values []string
	for {
		if ctx.Err() != nil || !*lockObtained || *stop > 0 {
			return
		}
		values = r.LRange(c, list, 0, asyncConsumerPage-1)
		if len(values) > 0 {
			handleAsyncEvents(ctx, c, list, db, r, values)
		}
		if len(values) < asyncConsumerPage {
			if !block || ctx.Err() != nil {
				return
			}
			time.Sleep(c.Engine().Registry().(*engineRegistryImplementation).asyncConsumerBlockTime)
		}
	}
}

func handleAsyncEvents(ctx context.Context, c Context, list string, db DB, r RedisCache, values []string) {
	operations := len(values)
	inTX := operations > 1
	func() {
		var d DBBase
		defer func() {
			if inTX && d != nil {
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
			err := handleAsyncEvent(c, d, event)
			if err != nil {
				if inTX {
					d.(DBTransaction).Rollback(c)
				}
				handleAsyncEventsOneByOne(ctx, c, list, db, r, values)
				return
			}
		}
		if inTX {
			d.(DBTransaction).Commit(c)
		}
		r.Ltrim(c, list, int64(len(values)), -1)
	}()
}

func handleAsyncEvent(c Context, db DBBase, value string) (err *mysql.MySQLError) {
	defer func() {
		if rec := recover(); rec != nil {
			asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
			if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
				err = asMySQLError
				return
			}
			panic(rec)
		}
	}()
	var data []any
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

func handleAsyncEventsOneByOne(ctx context.Context, c Context, list string, db DB, r RedisCache, values []string) {
	for _, event := range values {
		if ctx.Err() != nil {
			return
		}
		err := handleAsyncEvent(c, db, event)
		if err != nil {
			r.RPush(c, list+flushAsyncEventsListErrorSuffix, event, err.Error())
		}
		r.Ltrim(c, list, 1, -1)
	}
}
