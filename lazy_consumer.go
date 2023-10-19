package beeorm

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
)

const lazyConsumerPage = 1000
const lazyConsumerBlockTime = time.Second * 3

var mySQLErrorCodesToSkip = []uint16{
	1022, // Can't write; duplicate key in table '%s'
	1048, // Column '%s' cannot be null
	1049, // Unknown database '%s'
	1051, // Unknown table '%s'
	1054, // Unknown column '%s' in '%s'
	1062, // Duplicate entry '%s' for key %d
	1063, // Incorrect column specifier for column '%s'
	1067, // Invalid default value for '%s'
	1109, // Message: Unknown table '%s' in %s
	1146, // Table '%s.%s' doesn't exist
	1149, // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use
	2032, // Data truncated
}

const lazyConsumerLockName = "lazy_consumer"

func ConsumeLazyFlushEvents(c Context, block bool) error {
	lock, lockObtained := c.Engine().Redis(DefaultPoolCode).GetLocker().Obtain(c, lazyConsumerLockName, time.Minute, 0)
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
		r := c.Engine().Redis(schema.getForcedRedisCode())
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
		go consumeLazyEvents(c.Ctx(), ctxNoCancel.Clone(), schema.lazyCacheKey, db, r, block, waitGroup, &lockObtained)
	}
	waitGroup.Wait()
	return nil
}

func consumeLazyEvents(ctx context.Context, c Context, list string, db DB, r RedisCache,
	block bool, waitGroup *sync.WaitGroup, lockObtained *bool) {
	defer waitGroup.Done()
	var values []string
	for {
		if ctx.Err() != nil || !*lockObtained {
			return
		}
		values = r.LRange(c, list, 0, lazyConsumerPage-1)
		if len(values) > 0 {
			handleLazyEvents(ctx, c, list, db, r, values)
		}
		if len(values) < lazyConsumerPage {
			if !block || ctx.Err() != nil {
				return
			}
			time.Sleep(c.Engine().Registry().(*engineRegistryImplementation).lazyConsumerBlockTime)
		}
	}
}

func handleLazyEvents(ctx context.Context, c Context, list string, db DB, r RedisCache, values []string) {
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
				handleLazyEventsOneByOne(ctx, c, list, db, r, values)
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
			if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
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

func handleLazyEventsOneByOne(ctx context.Context, c Context, list string, db DB, r RedisCache, values []string) {
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
