package beeorm

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pkg/errors"

	_ "github.com/go-sql-driver/mysql" // force this mysql driver
)

type Registry struct {
	mysqlPools        map[string]MySQLPoolConfig
	mysqlTables       map[string]map[string]bool
	localCachePools   map[string]LocalCachePoolConfig
	redisPools        map[string]RedisPoolConfig
	entities          map[string]reflect.Type
	enums             map[string]Enum
	defaultEncoding   string
	defaultCollate    string
	redisStreamGroups map[string]map[string]map[string]bool
	redisStreamPools  map[string]string
	plugins           []Plugin
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Validate() (validated ValidatedRegistry, err error) {
	if r.defaultEncoding == "" {
		r.defaultEncoding = "utf8mb4"
	}
	if r.defaultCollate == "" {
		r.defaultCollate = "0900_ai_ci"
	}
	maxPoolLen := 0
	registry := &validatedRegistry{}
	registry.registry = r
	_, offset := time.Now().Zone()
	registry.timeOffset = int64(offset)
	l := len(r.entities)
	registry.entitySchemas = make(map[reflect.Type]*entitySchema, l)
	registry.entities = make(map[string]reflect.Type)
	if registry.mySQLServers == nil {
		registry.mySQLServers = make(map[string]MySQLPoolConfig)
	}
	for k, v := range r.mysqlPools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		db, err := sql.Open("mysql", v.GetDataSourceURI())
		checkError(err)
		var version string
		err = db.QueryRow("SELECT VERSION()").Scan(&version)
		checkError(err)
		v.(*mySQLPoolConfig).version, _ = strconv.Atoi(strings.Split(version, ".")[0])

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
		maxConnections = int(math.Max(math.Floor(float64(maxConnections)*0.9), 1))
		maxLimit := v.getMaxConnections()
		if maxLimit == 0 {
			maxLimit = maxConnections
		}
		maxLimit = int(math.Min(float64(maxConnections), float64(maxLimit)))
		waitTimeout = int(math.Max(float64(waitTimeout), 180))
		waitTimeout = int(math.Min(float64(waitTimeout), 180))
		db.SetMaxOpenConns(maxLimit)
		db.SetMaxIdleConns(maxLimit)
		db.SetConnMaxLifetime(time.Duration(waitTimeout) * time.Second)
		v.(*mySQLPoolConfig).client = db
		registry.mySQLServers[k] = v
	}
	if registry.localCacheServers == nil {
		registry.localCacheServers = make(map[string]LocalCachePoolConfig)
	}
	for k, v := range r.localCachePools {
		registry.localCacheServers[k] = v
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
	}
	if registry.redisServers == nil {
		registry.redisServers = make(map[string]RedisPoolConfig)
	}
	for k, v := range r.redisPools {
		registry.redisServers[k] = v
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
	}
	if registry.enums == nil {
		registry.enums = make(map[string]Enum)
	}
	for k, v := range r.enums {
		registry.enums[k] = v
	}
	for name, entityType := range r.entities {
		entitySchema := &entitySchema{}
		err := entitySchema.init(r, entityType)
		if err != nil {
			return nil, err
		}
		registry.entitySchemas[entityType] = entitySchema
		registry.entities[name] = entityType
	}
	_, has := r.redisStreamPools[LazyFlushChannelName]
	if !has {
		r.RegisterRedisStream(LazyFlushChannelName, "default")
		r.RegisterRedisStreamConsumerGroups(LazyFlushChannelName, LazyFlushGroupName)
	}
	if len(r.redisStreamGroups) > 0 {
		_, has = r.redisStreamPools[StreamGarbageCollectorChannelName]
		if !has {
			r.RegisterRedisStream(StreamGarbageCollectorChannelName, "default")
			r.RegisterRedisStreamConsumerGroups(StreamGarbageCollectorChannelName, StreamGarbageCollectorGroupName)
		}
	}
	registry.redisStreamGroups = r.redisStreamGroups
	registry.redisStreamPools = r.redisStreamPools
	registry.plugins = r.plugins
	registry.defaultQueryLogger = &defaultLogLogger{maxPoolLen: maxPoolLen, logger: log.New(os.Stderr, "", 0)}
	e := registry.CreateEngine()
	for _, schema := range registry.entitySchemas {
		_, err := checkStruct(schema, e.(*engineImplementation), schema.t, make(map[string]*IndexSchemaDefinition), nil, "")
		if err != nil {
			return nil, errors.Wrapf(err, "invalid entity struct '%s'", schema.t.String())
		}
		schema.registry = registry
	}
	return registry, nil
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

func (r *Registry) RegisterEntity(entity ...Entity) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		r.entities[t.String()] = t
	}
}

func (r *Registry) RegisterEnumStruct(code string, val interface{}, defaultValue ...string) {
	enum := initEnum(val, defaultValue...)
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = enum
}

func (r *Registry) RegisterEnum(code string, values []string, defaultValue ...string) {
	e := enum{}
	e.fields = values
	e.defaultValue = values[0]
	if len(defaultValue) > 0 {
		e.defaultValue = defaultValue[0]
	}
	e.mapping = make(map[string]int)
	for i, name := range values {
		e.mapping[name] = i + 1
	}
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = &e
}

func (r *Registry) RegisterMySQLPool(dataSourceName string, code ...string) {
	r.registerSQLPool(dataSourceName, code...)
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

func (r *Registry) RegisterLocalCache(size int, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	if r.localCachePools == nil {
		r.localCachePools = make(map[string]LocalCachePoolConfig)
	}
	r.localCachePools[dbCode] = newLocalCacheConfig(dbCode, size)
}

func (r *Registry) RegisterRedis(address, namespace string, db int, code ...string) {
	r.RegisterRedisWithCredentials(address, namespace, "", "", db, code...)
}

func (r *Registry) RegisterRedisWithCredentials(address, namespace, user, password string, db int, code ...string) {
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
	r.registerRedis(client, code, address, namespace, db)
}

func (r *Registry) RegisterRedisSentinel(masterName, namespace string, db int, sentinels []string, code ...string) {
	r.RegisterRedisSentinelWithCredentials(masterName, namespace, "", "", db, sentinels, code...)
}

func (r *Registry) RegisterRedisSentinelWithCredentials(masterName, namespace, user, password string, db int, sentinels []string, code ...string) {
	options := &redis.FailoverOptions{
		MasterName:      masterName,
		SentinelAddrs:   sentinels,
		DB:              db,
		ConnMaxIdleTime: time.Minute * 2,
		Username:        user,
		Password:        password,
	}
	client := redis.NewFailoverClient(options)
	r.registerRedis(client, code, fmt.Sprintf("%v", sentinels), namespace, db)
}

func (r *Registry) RegisterRedisSentinelWithOptions(namespace string, opts redis.FailoverOptions, db int, sentinels []string, code ...string) {
	opts.DB = db
	opts.SentinelAddrs = sentinels
	if opts.ConnMaxIdleTime == 0 {
		opts.ConnMaxIdleTime = time.Minute * 2
	}
	client := redis.NewFailoverClient(&opts)
	r.registerRedis(client, code, fmt.Sprintf("%v", sentinels), namespace, db)
}

func (r *Registry) RegisterRedisStream(name string, redisPool string) {
	if r.redisStreamGroups == nil {
		r.redisStreamGroups = make(map[string]map[string]map[string]bool)
		r.redisStreamPools = make(map[string]string)
	}
	_, has := r.redisStreamPools[name]
	if !has {
		r.redisStreamPools[name] = redisPool
	}
	if r.redisStreamGroups[redisPool] == nil {
		r.redisStreamGroups[redisPool] = make(map[string]map[string]bool)
	}
	if r.redisStreamGroups[redisPool][name] == nil {
		r.redisStreamGroups[redisPool][name] = make(map[string]bool)
	}
}

func (r *Registry) RegisterRedisStreamConsumerGroups(stream string, groups ...string) {
	if len(groups) == 0 {
		return
	}
	if r.redisStreamPools == nil || len(r.redisStreamPools[stream]) == 0 {
		panic(fmt.Errorf("redis stream %s is not registered", stream))
	}
	list, has := r.redisStreamGroups[r.redisStreamPools[stream]][stream]
	if !has {
		list = make(map[string]bool)
		r.redisStreamGroups[r.redisStreamPools[stream]][stream] = list
	}
	for _, name := range groups {
		list[name] = true
	}
}

func (r *Registry) registerSQLPool(dataSourceName string, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &mySQLPoolConfig{code: dbCode, dataSourceName: dataSourceName}
	if r.mysqlPools == nil {
		r.mysqlPools = make(map[string]MySQLPoolConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	dbName := strings.Split(parts[len(parts)-1], "?")[0]

	pos := strings.Index(dataSourceName, "limit_connections=")
	if pos > 0 {
		val := dataSourceName[pos+18:]
		val = strings.Split(val, "&")[0]
		db.maxConnections, _ = strconv.Atoi(val)
		dataSourceName = strings.Replace(dataSourceName, "limit_connections="+val, "", -1)
		dataSourceName = strings.Trim(dataSourceName, "?&")
		dataSourceName = strings.Replace(dataSourceName, "?&", "?", -1)
		db.dataSourceName = dataSourceName
	}
	db.databaseName = dbName
	r.mysqlPools[dbCode] = db
}

func (r *Registry) registerRedis(client *redis.Client, code []string, address, namespace string, db int) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &redisCacheConfig{code: dbCode, client: client, address: address, namespace: namespace,
		hasNamespace: namespace != "", db: db}
	if r.redisPools == nil {
		r.redisPools = make(map[string]RedisPoolConfig)
	}
	r.redisPools[dbCode] = redisCache
}

type RedisPoolConfig interface {
	GetCode() string
	GetDatabase() int
	GetAddress() string
	GetNamespace() string
	HasNamespace() bool
	getClient() *redis.Client
}

type redisCacheConfig struct {
	code         string
	client       *redis.Client
	db           int
	address      string
	namespace    string
	hasNamespace bool
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

func (p *redisCacheConfig) GetNamespace() string {
	return p.namespace
}

func (p *redisCacheConfig) HasNamespace() bool {
	return p.hasNamespace
}

func (p *redisCacheConfig) getClient() *redis.Client {
	return p.client
}
