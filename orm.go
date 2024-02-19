package beeorm

import (
	"context"
	"hash/maphash"
	"strings"
	"sync"

	"github.com/puzpuzpuz/xsync/v2"
)

type ID interface {
	int | uint | uint8 | uint16 | uint32 | uint64 | int8 | int16 | int32 | int64
}

type Meta map[string]string

func (m Meta) Get(key string) string {
	return m[key]
}

type ORM interface {
	Context() context.Context
	Clone() ORM
	CloneWithContext(context context.Context) ORM
	Engine() Engine
	Flush() error
	FlushAsync() error
	ClearFlush()
	RedisPipeLine(pool string) *RedisPipeLine
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetMetaData(key, value string)
	GetMetaData() Meta
	getDBLoggers() (bool, []LogHandler)
	getLocalCacheLoggers() (bool, []LogHandler)
	getRedisLoggers() (bool, []LogHandler)
	trackEntity(e EntityFlush)
}

type ormImplementation struct {
	context                context.Context
	engine                 *engineImplementation
	trackedEntities        *xsync.MapOf[uint64, *xsync.MapOf[uint64, EntityFlush]]
	queryLoggersDB         []LogHandler
	queryLoggersRedis      []LogHandler
	queryLoggersLocalCache []LogHandler
	hasRedisLogger         bool
	hasDBLogger            bool
	hasLocalCacheLogger    bool
	meta                   Meta
	stringBuilder          *strings.Builder
	stringBuilder2         *strings.Builder
	redisPipeLines         map[string]*RedisPipeLine
	flushDBActions         map[string][]dbAction
	flushPostActions       []func(orm ORM)
	mutexFlush             sync.Mutex
	mutexData              sync.Mutex
}

func (orm *ormImplementation) Context() context.Context {
	return orm.context
}

func (orm *ormImplementation) CloneWithContext(context context.Context) ORM {
	return &ormImplementation{
		context:                context,
		engine:                 orm.engine,
		queryLoggersDB:         orm.queryLoggersDB,
		queryLoggersRedis:      orm.queryLoggersRedis,
		queryLoggersLocalCache: orm.queryLoggersLocalCache,
		hasRedisLogger:         orm.hasRedisLogger,
		hasDBLogger:            orm.hasDBLogger,
		hasLocalCacheLogger:    orm.hasLocalCacheLogger,
		meta:                   orm.meta,
	}
}

func (orm *ormImplementation) Clone() ORM {
	return orm.CloneWithContext(orm.context)
}

func (orm *ormImplementation) RedisPipeLine(pool string) *RedisPipeLine {
	if orm.redisPipeLines != nil {
		pipeline, has := orm.redisPipeLines[pool]
		if has {
			return pipeline
		}
	}
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.redisPipeLines == nil {
		orm.redisPipeLines = make(map[string]*RedisPipeLine)
	}
	r := orm.engine.Redis(pool).(*redisCache)
	pipeline := &RedisPipeLine{orm: orm, pool: pool, r: r, pipeLine: r.client.Pipeline()}
	orm.redisPipeLines[pool] = pipeline
	return pipeline
}

func (orm *ormImplementation) SetMetaData(key, value string) {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.meta == nil {
		orm.meta = Meta{key: value}
		return
	}
	orm.meta[key] = value
}

func (orm *ormImplementation) GetMetaData() Meta {
	return orm.meta
}

func (orm *ormImplementation) Engine() Engine {
	return orm.engine
}

func (orm *ormImplementation) getRedisLoggers() (bool, []LogHandler) {
	if orm.hasRedisLogger {
		return true, orm.queryLoggersRedis
	}
	return false, nil
}

func (orm *ormImplementation) getDBLoggers() (bool, []LogHandler) {
	if orm.hasDBLogger {
		return true, orm.queryLoggersDB
	}
	return false, nil
}

func (orm *ormImplementation) getLocalCacheLoggers() (bool, []LogHandler) {
	if orm.hasLocalCacheLogger {
		return true, orm.queryLoggersLocalCache
	}
	return false, nil
}

func (orm *ormImplementation) trackEntity(e EntityFlush) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil {
		orm.trackedEntities = xsync.NewTypedMapOf[uint64, *xsync.MapOf[uint64, EntityFlush]](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	entities, loaded := orm.trackedEntities.LoadOrCompute(e.Schema().index, func() *xsync.MapOf[uint64, EntityFlush] {
		entities := xsync.NewTypedMapOf[uint64, EntityFlush](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		entities.Store(e.ID(), e)
		return entities
	})
	if loaded {
		entities.Store(e.ID(), e)
	}
}
