package beeorm

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pkg/errors"

	_ "github.com/go-sql-driver/mysql" // force this mysql driver
)

type Registry struct {
	oneAppMode      bool
	mysqlPools      map[string]MySQLPoolConfig
	mysqlTables     map[string]map[string]bool
	localCaches     map[string]LocalCache
	redisPools      map[string]RedisPoolConfig
	entities        map[string]reflect.Type
	defaultEncoding string
	defaultCollate  string
	plugins         []Plugin
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Validate() (Engine, error) {
	if r.defaultEncoding == "" {
		r.defaultEncoding = "utf8mb4"
	}
	if r.defaultCollate == "" {
		r.defaultCollate = "0900_ai_ci"
	}
	maxPoolLen := 0
	e := &engineImplementation{}
	e.registry = &engineRegistryImplementation{engine: e}
	e.registry.lazyConsumerBlockTime = lazyConsumerBlockTime
	e.registry.oneAppMode = r.oneAppMode
	l := len(r.entities)
	e.registry.entitySchemas = make(map[reflect.Type]*entitySchema, l)
	e.registry.entities = make(map[string]reflect.Type)
	e.registry.defaultDBCollate = r.defaultCollate
	e.registry.defaultDBEncoding = r.defaultEncoding
	e.registry.dbTables = r.mysqlTables
	if e.dbServers == nil {
		e.dbServers = make(map[string]DB)
	}
	for k, v := range r.mysqlPools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		db, err := sql.Open("mysql", v.GetDataSourceURI())
		checkError(err)
		checkError(err)

		var autoincrement uint64
		var maxConnections int
		var skip string
		err = db.QueryRow("SHOW VARIABLES LIKE 'auto_increment_increment'").Scan(&skip, &autoincrement)
		checkError(err)
		v.(*mySQLPoolConfig).autoincrement = autoincrement

		err = db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&skip, &maxConnections)
		checkError(err)
		var waitTimeout int
		err = db.QueryRow("SHOW VARIABLES LIKE 'wait_timeout'").Scan(&skip, &waitTimeout)
		checkError(err)

		maxLimit := 100
		if v.getPoolOptions().MaxOpenConnections > 0 {
			maxLimit = int(math.Min(float64(v.getPoolOptions().MaxOpenConnections), float64(maxConnections)))
		} else {
			maxLimit = int(math.Min(float64(maxLimit), float64(maxConnections)))
		}
		maxIdle := maxLimit
		if v.getPoolOptions().MaxIdleConnections > 0 {
			maxIdle = int(math.Min(float64(v.getPoolOptions().MaxIdleConnections), float64(maxLimit)))
		}
		maxDuration := 5 * time.Minute
		if v.getPoolOptions().ConnMaxLifetime > 0 {
			maxDuration = time.Duration(int(math.Min(v.getPoolOptions().ConnMaxLifetime.Seconds(), float64(waitTimeout)))) * time.Second
		} else {
			maxDuration = time.Duration(int(math.Min(maxDuration.Seconds(), float64(waitTimeout)))) * time.Second
		}
		db.SetMaxOpenConns(maxLimit)
		db.SetMaxIdleConns(maxIdle)
		db.SetConnMaxLifetime(maxDuration)
		v.(*mySQLPoolConfig).client = db
		e.dbServers[k] = &dbImplementation{config: v, client: &standardSQLClient{db: v.getClient()}}
	}
	if e.localCacheServers == nil {
		e.localCacheServers = make(map[string]LocalCache)
	}
	if e.redisServers == nil {
		e.redisServers = make(map[string]RedisCache)
	}
	for k, v := range r.redisPools {
		client := v.getClient()
		e.redisServers[k] = &redisCache{config: v, client: client}
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
	}
	for name, entityType := range r.entities {
		schema := &entitySchema{engine: e}
		err := schema.init(r, entityType)
		if err != nil {
			return nil, err
		}
		e.registry.entitySchemas[entityType] = schema
		e.registry.entities[name] = entityType
		if schema.hasLocalCache {
			r.localCaches[schema.GetCacheKey()] = newLocalCache(schema.GetCacheKey(), schema.localCacheLimit,
				true, len(schema.cachedReferences) > 0)
		}
	}
	for k, v := range r.localCaches {
		e.localCacheServers[k] = v
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
	}
	for _, schema := range e.registry.entitySchemas {
		if schema.hasLocalCache {
			schema.localCache = e.localCacheServers[schema.cacheKey].(*localCache)
		}
		if schema.hasRedisCache {
			schema.redisCache = e.redisServers[schema.redisCacheName].(*redisCache)
		}
	}
	e.registry.plugins = r.plugins
	e.registry.defaultQueryLogger = &defaultLogLogger{maxPoolLen: maxPoolLen, logger: log.New(os.Stderr, "", 0)}
	for _, schema := range e.registry.entitySchemas {
		_, err := checkStruct(e, schema, schema.t, make(map[string]*IndexSchemaDefinition), nil, "")
		if err != nil {
			return nil, errors.Wrapf(err, "invalid entity struct '%s'", schema.t.String())
		}
		schema.engine = e
	}
	return e, nil
}

func (r *Registry) SetDefaultEncoding(encoding string) {
	r.defaultEncoding = encoding
}

func (r *Registry) SetDefaultCollate(collate string) {
	r.defaultCollate = collate
}

func (r *Registry) GetDefaultCollate() string {
	return r.defaultCollate
}

func (r *Registry) RegisterPlugin(plugin Plugin) {
	interfaceInitRegistry, isInterfaceInitRegistry := plugin.(PluginInterfaceInitRegistry)
	if isInterfaceInitRegistry {
		interfaceInitRegistry.PluginInterfaceInitRegistry(r)
	}
	r.plugins = append(r.plugins, plugin)
}

func (r *Registry) RegisterEntity(entity ...any) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind().String() != "struct" {
			panic(fmt.Errorf("invalid entity definition, must be struct, %T provided", e))
		}
		r.entities[t.String()] = t
	}
}

type MySQLPoolOptions struct {
	ConnMaxLifetime    time.Duration
	MaxOpenConnections int
	MaxIdleConnections int
}

func (r *Registry) RegisterMySQLPool(dataSourceName string, poolOptions MySQLPoolOptions, code ...string) {
	r.registerSQLPool(dataSourceName, poolOptions, code...)
}

func (r *Registry) RegisterMySQLTable(pool string, tableName ...string) {
	if len(tableName) == 0 {
		return
	}
	if r.mysqlTables == nil {
		r.mysqlTables = map[string]map[string]bool{pool: {}}
	}
	if r.mysqlTables[pool] == nil {
		r.mysqlTables[pool] = map[string]bool{}
	}
	for _, table := range tableName {
		r.mysqlTables[pool][table] = true
	}
}

func (r *Registry) EnableOneAppMode() {
	r.oneAppMode = true
}

func (r *Registry) RegisterLocalCache(size int, code ...string) {
	dbCode := DefaultPoolCode
	if len(code) > 0 {
		dbCode = code[0]
	}
	if r.localCaches == nil {
		r.localCaches = make(map[string]LocalCache)
	}
	r.localCaches[dbCode] = newLocalCache(dbCode, size, false, false)
}

func (r *Registry) RegisterRedis(address string, db int, code ...string) {
	r.RegisterRedisWithCredentials(address, "", "", db, code...)
}

func (r *Registry) RegisterRedisWithCredentials(address, user, password string, db int, code ...string) {
	options := &redis.Options{
		Addr:            address,
		DB:              db,
		ConnMaxIdleTime: time.Minute * 2,
		Username:        user,
		Password:        password,
	}
	if strings.HasSuffix(address, ".sock") {
		options.Network = "unix"
	}
	client := redis.NewClient(options)
	r.registerRedis(client, code, address, db)
}

func (r *Registry) RegisterRedisSentinel(masterName string, db int, sentinels []string, code ...string) {
	r.RegisterRedisSentinelWithCredentials(masterName, "", "", db, sentinels, code...)
}

func (r *Registry) RegisterRedisSentinelWithCredentials(masterName, user, password string, db int, sentinels []string, code ...string) {
	options := &redis.FailoverOptions{
		MasterName:      masterName,
		SentinelAddrs:   sentinels,
		DB:              db,
		ConnMaxIdleTime: time.Minute * 2,
		Username:        user,
		Password:        password,
	}
	client := redis.NewFailoverClient(options)
	r.registerRedis(client, code, fmt.Sprintf("%v", sentinels), db)
}

func (r *Registry) RegisterRedisSentinelWithOptions(opts redis.FailoverOptions, db int, sentinels []string, code ...string) {
	opts.DB = db
	opts.SentinelAddrs = sentinels
	if opts.ConnMaxIdleTime == 0 {
		opts.ConnMaxIdleTime = time.Minute * 2
	}
	client := redis.NewFailoverClient(&opts)
	r.registerRedis(client, code, fmt.Sprintf("%v", sentinels), db)
}

func (r *Registry) registerSQLPool(dataSourceName string, poolOptions MySQLPoolOptions, code ...string) {
	dbCode := DefaultPoolCode
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &mySQLPoolConfig{code: dbCode, dataSourceName: dataSourceName, options: poolOptions}
	if r.mysqlPools == nil {
		r.mysqlPools = make(map[string]MySQLPoolConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	dbName := strings.Split(parts[len(parts)-1], "?")[0]
	db.databaseName = dbName
	r.mysqlPools[dbCode] = db
}

func (r *Registry) registerRedis(client *redis.Client, code []string, address string, db int) {
	dbCode := DefaultPoolCode
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &redisCacheConfig{code: dbCode, client: client, address: address, db: db}
	if r.redisPools == nil {
		r.redisPools = make(map[string]RedisPoolConfig)
	}
	r.redisPools[dbCode] = redisCache
}

type RedisPoolConfig interface {
	GetCode() string
	GetDatabase() int
	GetAddress() string
	getClient() *redis.Client
}

type redisCacheConfig struct {
	code    string
	client  *redis.Client
	db      int
	address string
}

func (p *redisCacheConfig) GetCode() string {
	return p.code
}

func (p *redisCacheConfig) GetDatabase() int {
	return p.db
}

func (p *redisCacheConfig) GetAddress() string {
	return p.address
}

func (p *redisCacheConfig) getClient() *redis.Client {
	return p.client
}
