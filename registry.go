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

type Registry interface {
	Validate() (Engine, error)
	RegisterEntity(entity ...any)
	RegisterPlugin(plugin ...any)
	RegisterMySQL(dataSourceName string, poolCode string, poolOptions *MySQLOptions)
	RegisterLocalCache(code string, limit int)
	RegisterRedis(address string, db int, poolCode string, options *RedisOptions)
	InitByYaml(yaml map[string]any) error
	SetOption(key string, value any)
}

type registry struct {
	mysqlPools  map[string]MySQLConfig
	localCaches map[string]LocalCache
	redisPools  map[string]RedisPoolConfig
	entities    map[string]reflect.Type
	plugins     []any
	options     map[string]any
}

func NewRegistry() Registry {
	return &registry{}
}

func (r *registry) Validate() (Engine, error) {
	maxPoolLen := 0
	e := &engineImplementation{}
	e.registry = &engineRegistryImplementation{engine: e}
	e.registry.options = make(map[string]any)
	e.registry.asyncConsumerBlockTime = asyncConsumerBlockTime
	l := len(r.entities)
	e.registry.entitySchemas = make(map[reflect.Type]*entitySchema, l)
	e.registry.entitySchemasQuickMap = make(map[reflect.Type]*entitySchema, l)
	e.registry.entityLogSchemas = make(map[reflect.Type]*entitySchema, l)
	e.registry.entities = make(map[string]reflect.Type)
	e.registry.enums = make(map[string][]string)
	e.options = make(map[string]any)
	if e.dbServers == nil {
		e.dbServers = make(map[string]DB)
	}
	e.registry.dbTables = make(map[string]map[string]bool)
	for k, v := range r.mysqlPools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		db, err := sql.Open("mysql", v.GetDataSourceURI())
		if err != nil {
			return nil, err
		}

		var maxConnections int
		var skip string
		err = db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&skip, &maxConnections)
		if err != nil {
			return nil, err
		}
		var waitTimeout int
		err = db.QueryRow("SHOW VARIABLES LIKE 'wait_timeout'").Scan(&skip, &waitTimeout)
		if err != nil {
			return nil, err
		}

		maxLimit := 100
		if v.GetOptions().MaxOpenConnections > 0 {
			maxLimit = int(math.Min(float64(v.GetOptions().MaxOpenConnections), float64(maxConnections)))
		} else {
			maxLimit = int(math.Min(float64(maxLimit), float64(maxConnections)))
		}
		maxIdle := maxLimit
		if v.GetOptions().MaxIdleConnections > 0 {
			maxIdle = int(math.Min(float64(v.GetOptions().MaxIdleConnections), float64(maxLimit)))
		}
		maxDuration := 5 * time.Minute
		if v.GetOptions().ConnMaxLifetime > 0 {
			maxDuration = time.Duration(int(math.Min(v.GetOptions().ConnMaxLifetime.Seconds(), float64(waitTimeout)))) * time.Second
		} else {
			maxDuration = time.Duration(int(math.Min(maxDuration.Seconds(), float64(waitTimeout)))) * time.Second
		}
		db.SetMaxOpenConns(maxLimit)
		db.SetMaxIdleConns(maxIdle)
		db.SetConnMaxLifetime(maxDuration)
		options := v.GetOptions()
		if options.DefaultEncoding == "" {
			options.DefaultEncoding = "utf8mb4"
		}
		if options.DefaultCollate == "" {
			options.DefaultCollate = "0900_ai_ci"
		}
		if len(options.IgnoredTables) > 0 {
			if e.registry.dbTables[v.GetCode()] == nil {
				e.registry.dbTables[v.GetCode()] = make(map[string]bool)
			}
			for _, ignoredTable := range options.IgnoredTables {
				e.registry.dbTables[v.GetCode()][ignoredTable] = true
			}
		}
		v.(*mySQLConfig).client = db
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
	index := uint64(0)
	for name, entityType := range r.entities {
		schema := &entitySchema{engine: e, index: index}
		index++
		err := schema.init(r, entityType)
		if err != nil {
			return nil, err
		}
		e.registry.entitySchemas[entityType] = schema
		e.registry.entitySchemasQuickMap[entityType] = schema
		e.registry.entitySchemasQuickMap[reflect.PointerTo(entityType)] = schema
		e.registry.entities[name] = entityType
		if schema.hasLocalCache {
			r.localCaches[schema.getCacheKey()] = newLocalCache(schema.getCacheKey(), schema.localCacheLimit, schema)
		}
		extractEnums(schema.fields, e.registry)
	}
	for _, entityType := range r.entities {
		logEntity, isLogEntity := reflect.New(entityType).Interface().(logEntityInterface)
		if isLogEntity {
			logSchema := e.registry.entitySchemas[entityType]
			targetType := logEntity.getLogEntityTarget()
			targetSchema := e.registry.entitySchemas[targetType]
			logPool := targetSchema.getTag("log-pool", "default", "")
			if logPool != "" {
				logSchema.mysqlPoolCode = logPool
			}
			logSchema.tableName = "_LogEntity_" + targetSchema.mysqlPoolCode + "_" + targetType.Name()
			e.registry.entityLogSchemas[targetType] = logSchema
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
	e.registry.defaultQueryLogger = &defaultLogLogger{maxPoolLen: maxPoolLen, logger: log.New(os.Stderr, "", 0)}
	for _, schema := range e.registry.entitySchemas {
		_, err := checkStruct(e, schema, schema.t, make(map[string]*IndexSchemaDefinition), nil, "", -1)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid entity struct '%s'", schema.t.String())
		}
		schema.engine = e
	}
	for _, plugin := range r.plugins {
		pluginInterfaceValidateRegistry, isInterface := plugin.(PluginInterfaceValidateRegistry)
		if isInterface {
			err := pluginInterfaceValidateRegistry.ValidateRegistry(e, r)
			if err != nil {
				return nil, err
			}
		}
		pluginInterfaceEntityFlush, isInterface := plugin.(PluginInterfaceEntityFlush)
		if isInterface {
			e.pluginFlush = append(e.pluginFlush, pluginInterfaceEntityFlush)
		}
	}
	for key, value := range r.options {
		e.registry.options[key] = value
	}
	return e, nil
}

func (r *registry) SetOption(key string, value any) {
	if r.options == nil {
		r.options = map[string]any{key: value}
		return
	}
	r.options[key] = value
}

func (r *registry) RegisterEntity(entity ...any) {
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

func (r *registry) RegisterPlugin(plugin ...any) {
	r.plugins = append(r.plugins, plugin...)
}

type MySQLOptions struct {
	ConnMaxLifetime    time.Duration
	MaxOpenConnections int
	MaxIdleConnections int
	DefaultEncoding    string
	DefaultCollate     string
	IgnoredTables      []string
}

func (r *registry) RegisterMySQL(dataSourceName string, poolCode string, poolOptions *MySQLOptions) {
	db := &mySQLConfig{code: poolCode, dataSourceName: dataSourceName, options: poolOptions}
	if r.mysqlPools == nil {
		r.mysqlPools = make(map[string]MySQLConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	dbName := strings.Split(parts[len(parts)-1], "?")[0]
	db.databaseName = dbName
	r.mysqlPools[poolCode] = db
}

func (r *registry) RegisterLocalCache(code string, limit int) {
	if r.localCaches == nil {
		r.localCaches = make(map[string]LocalCache)
	}
	r.localCaches[code] = newLocalCache(code, limit, nil)
}

type RedisOptions struct {
	User            string
	Password        string
	Master          string
	Sentinels       []string
	SentinelOptions *redis.FailoverOptions
}

func (r *registry) RegisterRedis(address string, db int, poolCode string, options *RedisOptions) {
	if options != nil && len(options.Sentinels) > 0 {
		sentinelOptions := options.SentinelOptions
		if sentinelOptions == nil {
			sentinelOptions = &redis.FailoverOptions{
				MasterName:      options.Master,
				SentinelAddrs:   options.Sentinels,
				DB:              db,
				ConnMaxIdleTime: time.Minute * 2,
				Username:        options.User,
				Password:        options.Password,
			}
		}
		client := redis.NewFailoverClient(sentinelOptions)
		r.registerRedis(client, poolCode, fmt.Sprintf("%v", options.Sentinels), db)
		return
	}
	redisOptions := &redis.Options{
		Addr:            address,
		DB:              db,
		ConnMaxIdleTime: time.Minute * 2,
	}
	if options != nil {
		redisOptions.Username = options.User
		redisOptions.Password = options.Password
	}
	if strings.HasSuffix(address, ".sock") {
		redisOptions.Network = "unix"
	}
	client := redis.NewClient(redisOptions)
	r.registerRedis(client, poolCode, address, db)
}

func (r *registry) registerRedis(client *redis.Client, code string, address string, db int) {
	redisPool := &redisCacheConfig{code: code, client: client, address: address, db: db}
	if r.redisPools == nil {
		r.redisPools = make(map[string]RedisPoolConfig)
	}
	r.redisPools[code] = redisPool
}

type RedisPoolConfig interface {
	GetCode() string
	GetDatabaseNumber() int
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

func (p *redisCacheConfig) GetDatabaseNumber() int {
	return p.db
}

func (p *redisCacheConfig) GetAddress() string {
	return p.address
}

func (p *redisCacheConfig) getClient() *redis.Client {
	return p.client
}

func extractEnums(fields *tableFields, r *engineRegistryImplementation) {
	for _, enum := range fields.enums {
		if r.enums[enum.name] == nil {
			r.enums[enum.name] = enum.fields
		}
	}
	for _, s := range fields.structsFields {
		extractEnums(s, r)
	}
	for _, s := range fields.structsFieldsArray {
		extractEnums(s, r)
	}
}
