package beeorm

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const entityIndexerPage = 5000

type CachedQuery struct{}

type cachedQueryDefinition struct {
	Max           int
	Query         string
	TrackedFields []string
	QueryFields   []string
	OrderFields   []string
}

type Enum interface {
	GetFields() []string
	GetDefault() string
	Has(value string) bool
	Index(value string) int
}

type enum struct {
	fields       []string
	mapping      map[string]int
	defaultValue string
}

func (enum *enum) GetFields() []string {
	return enum.fields
}

func (enum *enum) GetDefault() string {
	return enum.defaultValue
}

func (enum *enum) Has(value string) bool {
	_, has := enum.mapping[value]
	return has
}

func (enum *enum) Index(value string) int {
	return enum.mapping[value]
}

func initEnum(ref interface{}, defaultValue ...string) *enum {
	enum := &enum{}
	e := reflect.ValueOf(ref)
	enum.mapping = make(map[string]int)
	enum.fields = make([]string, 0)
	for i := 0; i < e.Type().NumField(); i++ {
		name := e.Field(i).String()
		enum.fields = append(enum.fields, name)
		enum.mapping[name] = i + 1
	}
	if len(defaultValue) > 0 {
		enum.defaultValue = defaultValue[0]
	} else {
		enum.defaultValue = enum.fields[0]
	}
	return enum
}

type TableSchema interface {
	GetTableName() string
	GetType() reflect.Type
	NewEntity() Entity
	DropTable(engine *Engine)
	TruncateTable(engine *Engine)
	UpdateSchema(engine *Engine)
	ReindexRedisSearchIndex(engine *Engine)
	UpdateSchemaAndTruncateTable(engine *Engine)
	GetMysql(engine *Engine) *DB
	GetLocalCache(engine *Engine) (cache *LocalCache, has bool)
	GetRedisCache(engine *Engine) (cache *RedisCache, has bool)
	GetRedisSearch(engine *Engine) (search *RedisSearch, has bool)
	GetReferences() []string
	GetColumns() []string
	GetSchemaChanges(engine *Engine) (has bool, alters []Alter)
}

type tableSchema struct {
	tableName            string
	mysqlPoolName        string
	t                    reflect.Type
	fields               *tableFields
	registry             *validatedRegistry
	fieldsQuery          string
	tags                 map[string]map[string]string
	cachedIndexes        map[string]*cachedQueryDefinition
	cachedIndexesOne     map[string]*cachedQueryDefinition
	cachedIndexesAll     map[string]*cachedQueryDefinition
	columnNames          []string
	columnMapping        map[string]int
	uniqueIndices        map[string][]string
	uniqueIndicesGlobal  map[string][]string
	dirtyFields          map[string][]string
	refOne               []string
	refMany              []string
	idIndex              int
	localCacheName       string
	hasLocalCache        bool
	redisCacheName       string
	hasRedisCache        bool
	searchCacheName      string
	hasSearchCache       bool
	cachePrefix          string
	hasFakeDelete        bool
	hasLog               bool
	logPoolName          string //name of redis
	logTableName         string
	skipLogs             []string
	redisSearchPrefix    string
	redisSearchIndex     *RedisSearchIndex
	mapBindToRedisSearch mapBindToRedisSearch
}

type mapBindToRedisSearch map[string]func(val interface{}) interface{}
type mapBindToScanPointer map[string]func() interface{}
type mapPointerToValue map[string]func(val interface{}) interface{}

type tableFields struct {
	t                       reflect.Type
	fields                  map[int]reflect.StructField
	prefix                  string
	uintegers               []int
	integers                []int
	uintegersNullable       []int
	uintegersNullableSize   []int
	integersNullable        []int
	integersNullableSize    []int
	strings                 []int
	stringsEnums            []int
	enums                   []Enum
	sliceStringsSets        []int
	sets                    []Enum
	bytes                   []int
	fakeDelete              int
	booleans                []int
	booleansNullable        []int
	floats                  []int
	floatsPrecision         []int
	floatsNullable          []int
	floatsNullablePrecision []int
	floatsNullableSize      []int
	timesNullable           []int
	datesNullable           []int
	times                   []int
	dates                   []int
	jsons                   []int
	structs                 []int
	structsFields           []*tableFields
	refs                    []int
	refsTypes               []reflect.Type
	refsMany                []int
	refsManyTypes           []reflect.Type
}

func getTableSchema(registry *validatedRegistry, entityType reflect.Type) *tableSchema {
	return registry.tableSchemas[entityType]
}

func (tableSchema *tableSchema) GetTableName() string {
	return tableSchema.tableName
}

func (tableSchema *tableSchema) GetType() reflect.Type {
	return tableSchema.t
}

func (tableSchema *tableSchema) DropTable(engine *Engine) {
	pool := tableSchema.GetMysql(engine)
	pool.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName))
}

func (tableSchema *tableSchema) ReindexRedisSearchIndex(engine *Engine) {
	if tableSchema.redisSearchIndex != nil {
		engine.GetRedisSearch(tableSchema.searchCacheName).ForceReindex(tableSchema.redisSearchIndex.Name)
	}
}

func (tableSchema *tableSchema) TruncateTable(engine *Engine) {
	pool := tableSchema.GetMysql(engine)
	_ = pool.Exec(fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName))
	_ = pool.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName))
}

func (tableSchema *tableSchema) UpdateSchema(engine *Engine) {
	pool := tableSchema.GetMysql(engine)
	has, alters := tableSchema.GetSchemaChanges(engine)
	if has {
		for _, alter := range alters {
			_ = pool.Exec(alter.SQL)
		}
	}
}

func (tableSchema *tableSchema) UpdateSchemaAndTruncateTable(engine *Engine) {
	tableSchema.UpdateSchema(engine)
	pool := tableSchema.GetMysql(engine)
	_ = pool.Exec(fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName))
	_ = pool.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetPoolConfig().GetDatabase(), tableSchema.tableName))
}

func (tableSchema *tableSchema) GetMysql(engine *Engine) *DB {
	return engine.GetMysql(tableSchema.mysqlPoolName)
}

func (tableSchema *tableSchema) GetLocalCache(engine *Engine) (cache *LocalCache, has bool) {
	if !tableSchema.hasLocalCache {
		return nil, false
	}
	return engine.GetLocalCache(tableSchema.localCacheName), true
}

func (tableSchema *tableSchema) GetRedisCache(engine *Engine) (cache *RedisCache, has bool) {
	if !tableSchema.hasRedisCache {
		return nil, false
	}
	return engine.GetRedis(tableSchema.redisCacheName), true
}

func (tableSchema *tableSchema) GetRedisSearch(engine *Engine) (search *RedisSearch, has bool) {
	if !tableSchema.hasSearchCache {
		return nil, false
	}
	return engine.GetRedisSearch(tableSchema.searchCacheName), true
}

func (tableSchema *tableSchema) GetReferences() []string {
	return tableSchema.refOne
}

func (tableSchema *tableSchema) GetColumns() []string {
	return tableSchema.columnNames
}

func (tableSchema *tableSchema) GetSchemaChanges(engine *Engine) (has bool, alters []Alter) {
	return getSchemaChanges(engine, tableSchema)
}

func initTableSchema(registry *Registry, entityType reflect.Type) (*tableSchema, error) {
	tags := extractTags(registry, entityType, "")
	oneRefs := make([]string, 0)
	manyRefs := make([]string, 0)
	mapBindToRedisSearch := mapBindToRedisSearch{}
	mapBindToScanPointer := mapBindToScanPointer{}
	mapPointerToValue := mapPointerToValue{}
	mysql, has := tags["ORM"]["mysql"]
	if !has {
		mysql = "default"
	}
	_, has = registry.mysqlPools[mysql]
	if !has {
		return nil, fmt.Errorf("mysql pool '%s' not found", mysql)
	}
	table, has := tags["ORM"]["table"]
	if !has {
		table = entityType.Name()
	}
	localCache := getTagValue(tags, "localCache")
	redisCache := getTagValue(tags, "redisCache")
	redisSearch := getTagValue(tags, "redisSearch")
	if localCache != "" {
		_, has = registry.localCachePools[localCache]
		if !has {
			return nil, fmt.Errorf("local cache pool '%s' not found", localCache)
		}
	}
	if redisCache != "" {
		_, has = registry.mysqlPools[redisCache]
		if !has {
			return nil, fmt.Errorf("redis pool '%s' not found", redisCache)
		}
	}
	if redisSearch != "" {
		_, has = registry.redisPools[redisSearch]
		if !has {
			return nil, fmt.Errorf("redis pool '%s' not found", redisSearch)
		}
	} else {
		redisSearch = "default"
	}
	cachePrefix := ""
	if mysql != "default" {
		cachePrefix = mysql
	}
	cachePrefix += table
	cachedQueries := make(map[string]*cachedQueryDefinition)
	cachedQueriesOne := make(map[string]*cachedQueryDefinition)
	cachedQueriesAll := make(map[string]*cachedQueryDefinition)
	dirtyFields := make(map[string][]string)
	hasFakeDelete := false
	fakeDeleteField, has := entityType.FieldByName("FakeDelete")
	if has && fakeDeleteField.Type.String() == "bool" {
		hasFakeDelete = true
	}
	for key, values := range tags {
		isOne := false
		query, has := values["query"]
		if !has {
			query, has = values["queryOne"]
			isOne = true
		}
		queryOrigin := query
		fields := make([]string, 0)
		fieldsTracked := make([]string, 0)
		fieldsQuery := make([]string, 0)
		fieldsOrder := make([]string, 0)
		if has {
			re := regexp.MustCompile(":([A-Za-z0-9])+")
			variables := re.FindAllString(query, -1)
			for _, variable := range variables {
				fieldName := variable[1:]
				has := false
				for _, v := range fields {
					if v == fieldName {
						has = true
						break
					}
				}
				if !has {
					fields = append(fields, fieldName)
				}
				query = strings.Replace(query, variable, fmt.Sprintf("`%s`", fieldName), 1)
			}
			if hasFakeDelete && len(variables) > 0 {
				fields = append(fields, "FakeDelete")
			}
			if query == "" {
				if hasFakeDelete {
					query = "`FakeDelete` = 0 ORDER BY `ID`"
				} else {
					query = "1 ORDER BY `ID`"
				}
			} else if hasFakeDelete {
				query = "`FakeDelete` = 0 AND " + query
			}
			queryLower := strings.ToLower(queryOrigin)
			posOrderBy := strings.Index(queryLower, "order by")
			for _, f := range fields {
				if f != "ID" {
					fieldsTracked = append(fieldsTracked, f)
				}
				pos := strings.Index(queryOrigin, ":"+f)
				if pos < posOrderBy || posOrderBy == -1 {
					fieldsQuery = append(fieldsQuery, f)
				}
			}
			if posOrderBy > -1 {
				variables = re.FindAllString(queryOrigin[posOrderBy:], -1)
				for _, variable := range variables {
					fieldName := variable[1:]
					fieldsOrder = append(fieldsOrder, fieldName)
				}
			}

			if !isOne {
				def := &cachedQueryDefinition{50000, query, fieldsTracked, fieldsQuery, fieldsOrder}
				cachedQueries[key] = def
				cachedQueriesAll[key] = def
			} else {
				def := &cachedQueryDefinition{1, query, fieldsTracked, fieldsQuery, fieldsOrder}
				cachedQueriesOne[key] = def
				cachedQueriesAll[key] = def
			}
		}
		_, has = values["ref"]
		if has {
			oneRefs = append(oneRefs, key)
		}
		_, has = values["refs"]
		if has {
			manyRefs = append(manyRefs, key)
		}
		dirtyValues, has := values["dirty"]
		if has {
			for _, v := range strings.Split(dirtyValues, ",") {
				dirtyFields[v] = append(dirtyFields[v], key)
			}
		}
	}
	logPoolName := tags["ORM"]["log"]
	if logPoolName == "true" {
		logPoolName = mysql
	}
	uniqueIndices := make(map[string]map[int]string)
	uniqueIndicesSimple := make(map[string][]string)
	uniqueIndicesSimpleGlobal := make(map[string][]string)
	indices := make(map[string]map[int]string)
	skipLogs := make([]string, 0)
	uniqueGlobal, has := tags["ORM"]["unique"]
	if has {
		parts := strings.Split(uniqueGlobal, "|")
		for _, part := range parts {
			def := strings.Split(part, ":")
			uniqueIndices[def[0]] = make(map[int]string)
			uniqueIndicesSimple[def[0]] = make([]string, 0)
			uniqueIndicesSimpleGlobal[def[0]] = make([]string, 0)
			for i, field := range strings.Split(def[1], ",") {
				uniqueIndices[def[0]][i+1] = field
				uniqueIndicesSimple[def[0]] = append(uniqueIndicesSimple[def[0]], field)
				uniqueIndicesSimpleGlobal[def[0]] = append(uniqueIndicesSimpleGlobal[def[0]], field)
			}
		}
	}
	for k, v := range tags {
		keys, has := v["unique"]
		if has && k != "ORM" {
			values := strings.Split(keys, ",")
			for _, indexName := range values {
				parts := strings.Split(indexName, ":")
				id := int64(1)
				if len(parts) > 1 {
					id, _ = strconv.ParseInt(parts[1], 10, 64)
				}
				if uniqueIndices[parts[0]] == nil {
					uniqueIndices[parts[0]] = make(map[int]string)
				}
				uniqueIndices[parts[0]][int(id)] = k
				if uniqueIndicesSimple[parts[0]] == nil {
					uniqueIndicesSimple[parts[0]] = make([]string, 0)
				}
				uniqueIndicesSimple[parts[0]] = append(uniqueIndicesSimple[parts[0]], k)
			}
		}
		keys, has = v["index"]
		if has {
			values := strings.Split(keys, ",")
			for _, indexName := range values {
				parts := strings.Split(indexName, ":")
				id := int64(1)
				if len(parts) > 1 {
					id, _ = strconv.ParseInt(parts[1], 10, 64)
				}
				if indices[parts[0]] == nil {
					indices[parts[0]] = make(map[int]string)
				}
				indices[parts[0]][int(id)] = k
			}
		}
		_, has = v["skip-log"]
		if has {
			skipLogs = append(skipLogs, k)
		}
	}
	for _, ref := range oneRefs {
		has := false
		for _, v := range indices {
			if v[1] == ref {
				has = true
				break
			}
		}
		if !has {
			for _, v := range uniqueIndices {
				if v[1] == ref {
					has = true
					break
				}
			}
			if !has {
				indices["_"+ref] = map[int]string{1: ref}
			}
		}
	}
	redisSearchIndex := &RedisSearchIndex{}
	fields := buildTableFields(entityType, registry, redisSearchIndex, mapBindToRedisSearch, mapBindToScanPointer,
		mapPointerToValue, 1, "", tags)
	searchPrefix := ""
	if len(redisSearchIndex.Fields) > 0 {
		redisSearchIndex.Name = entityType.String()
		redisSearchIndex.RedisPool = redisSearch
		searchPrefix = fmt.Sprintf("%x", sha256.Sum256([]byte(entityType.String())))
		searchPrefix = searchPrefix[0:5] + ":"
		redisSearchIndex.Prefixes = []string{searchPrefix}
		redisSearchIndex.NoOffsets = true
		redisSearchIndex.NoFreqs = true
		redisSearchIndex.NoNHL = true
		indexQuery := "SELECT `ID`"
		indexColumns := make([]string, 0)
		for column := range mapBindToRedisSearch {
			indexQuery += ",`" + column + "`"
			indexColumns = append(indexColumns, column)
		}
		indexQuery += " FROM `" + table + "` WHERE `ID` > ?"
		if hasFakeDelete {
			indexQuery += " AND FakeDelete = 0"
		}
		indexQuery += " ORDER BY `ID` LIMIT " + strconv.Itoa(entityIndexerPage)
		redisSearchIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
			results, def := engine.GetMysql(mysql).Query(indexQuery, lastID)
			defer def()
			total := 0
			pointers := make([]interface{}, len(indexColumns)+1)
			v := uint64(0)
			pointers[0] = &v
			for i, column := range indexColumns {
				pointers[i+1] = mapBindToScanPointer[column]()
			}
			for results.Next() {
				results.Scan(pointers...)
				lastID = *pointers[0].(*uint64)
				pusher.NewDocument(redisSearchIndex.Prefixes[0] + strconv.FormatUint(lastID, 10))
				for i, column := range indexColumns {
					val := mapPointerToValue[column](pointers[i+1])
					pusher.setField(column, mapBindToRedisSearch[column](val))
				}
				pusher.PushDocument()
				total++
			}
			return lastID, total == entityIndexerPage
		}
	} else {
		redisSearchIndex = nil
	}
	columns, fieldsQuery := fields.getColumnNames()
	columnMapping := make(map[string]int)
	idIndex := 0
	for i, name := range columns {
		columnMapping[name] = i
		if name == "ID" {
			idIndex = i
		}
	}
	cachePrefix = fmt.Sprintf("%x", sha256.Sum256([]byte(cachePrefix+fieldsQuery)))
	cachePrefix = cachePrefix[0:5]
	if redisSearchIndex == nil {
		redisSearch = ""
	}
	tableSchema := &tableSchema{tableName: table,
		mysqlPoolName:        mysql,
		t:                    entityType,
		fields:               fields,
		fieldsQuery:          fieldsQuery,
		redisSearchPrefix:    searchPrefix,
		redisSearchIndex:     redisSearchIndex,
		mapBindToRedisSearch: mapBindToRedisSearch,
		tags:                 tags,
		idIndex:              idIndex,
		columnNames:          columns,
		columnMapping:        columnMapping,
		cachedIndexes:        cachedQueries,
		cachedIndexesOne:     cachedQueriesOne,
		cachedIndexesAll:     cachedQueriesAll,
		dirtyFields:          dirtyFields,
		localCacheName:       localCache,
		hasLocalCache:        localCache != "",
		redisCacheName:       redisCache,
		hasRedisCache:        redisCache != "",
		searchCacheName:      redisSearch,
		hasSearchCache:       redisSearchIndex != nil,
		refOne:               oneRefs,
		refMany:              manyRefs,
		cachePrefix:          cachePrefix,
		uniqueIndices:        uniqueIndicesSimple,
		uniqueIndicesGlobal:  uniqueIndicesSimpleGlobal,
		hasFakeDelete:        hasFakeDelete,
		hasLog:               logPoolName != "",
		logPoolName:          logPoolName,
		logTableName:         fmt.Sprintf("_log_%s_%s", mysql, table),
		skipLogs:             skipLogs}

	all := make(map[string]map[int]string)
	for k, v := range uniqueIndices {
		all[k] = v
	}
	for k, v := range indices {
		all[k] = v
	}
	for k, v := range all {
		for k2, v2 := range all {
			if k == k2 {
				continue
			}
			same := 0
			for i := 1; i <= len(v); i++ {
				right, has := v2[i]
				if has && right == v[i] {
					same++
					continue
				}
				break
			}
			if same == len(v) {
				return nil, fmt.Errorf("duplicated index %s with %s in %s", k, k2, entityType.String())
			}
		}
	}
	for k, v := range tableSchema.cachedIndexesOne {
		ok := false
		for _, columns := range uniqueIndices {
			if len(columns) != len(v.QueryFields) {
				continue
			}
			valid := 0
			for _, field1 := range v.QueryFields {
				for _, field2 := range columns {
					if field1 == field2 {
						valid++
					}
				}
			}
			if valid == len(columns) {
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("missing unique index for cached query '%s' in %s", k, entityType.String())
		}
	}
	for k, v := range tableSchema.cachedIndexes {
		if v.Query == "1 ORDER BY `ID`" {
			continue
		}
		//first do we have query fields
		ok := false
		for _, columns := range all {
			valid := 0
			for _, field1 := range v.QueryFields {
				for _, field2 := range columns {
					if field1 == field2 {
						valid++
					}
				}
			}
			if valid == len(v.QueryFields) {
				if len(v.OrderFields) == 0 {
					ok = true
					break
				}
				valid := 0
				key := len(columns)
				if columns[len(columns)] == "FakeDelete" {
					key--
				}
				for i := len(v.OrderFields); i > 0; i-- {
					if columns[key] == v.OrderFields[i-1] {
						valid++
						key--
						continue
					}
					break
				}
				if valid == len(v.OrderFields) {
					ok = true
				}
			}
		}
		if !ok {
			return nil, fmt.Errorf("missing index for cached query '%s' in %s", k, entityType.String())
		}
	}
	return tableSchema, nil
}

func getTagValue(tags map[string]map[string]string, key string) string {
	userValue, has := tags["ORM"][key]
	if has {
		if userValue == "true" {
			return "default"
		}
		return userValue
	}
	return ""
}

func buildTableFields(t reflect.Type, registry *Registry, index *RedisSearchIndex,
	mapBindToRedisSearch mapBindToRedisSearch, mapBindToScanPointer mapBindToScanPointer, mapPointerToValue mapPointerToValue,
	start int, prefix string, schemaTags map[string]map[string]string) *tableFields {
	fields := &tableFields{t: t, prefix: prefix, fields: make(map[int]reflect.StructField)}
	for i := start; i < t.NumField(); i++ {
		f := t.Field(i)
		fields.fields[i] = f
		tags := schemaTags[f.Name]
		typeName := f.Type.String()
		_, has := tags["ignore"]
		if has {
			continue
		}
		_, hasSearchable := tags["searchable"]
		_, hasSortable := tags["sortable"]
		switch typeName {
		case "uint",
			"uint8",
			"uint16",
			"uint32",
			"uint64":
			fields.uintegers = append(fields.uintegers, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				if hasSortable && typeName == "uint64" {
					mapBindToRedisSearch[prefix+f.Name] = func(val interface{}) interface{} {
						if val.(uint64) > math.MaxInt32 {
							panic(errors.New("integer too high for redis search sort field"))
						}
						return val
					}
				} else {
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapper
				}
			}
			mapBindToScanPointer[prefix+f.Name] = func() interface{} {
				v := uint64(0)
				return &v
			}
			mapPointerToValue[prefix+f.Name] = func(val interface{}) interface{} {
				return *val.(*uint64)
			}
		case "*uint",
			"*uint8",
			"*uint16",
			"*uint32",
			"*uint64":
			fields.uintegersNullable = append(fields.uintegersNullable, i)
			switch typeName {
			case "*uint":
				fields.uintegersNullableSize = append(fields.uintegersNullableSize, 0)
			case "*uint8":
				fields.uintegersNullableSize = append(fields.uintegersNullableSize, 8)
			case "*uint16":
				fields.uintegersNullableSize = append(fields.uintegersNullableSize, 16)
			case "*uint32":
				fields.uintegersNullableSize = append(fields.uintegersNullableSize, 32)
			case "*uint64":
				fields.uintegersNullableSize = append(fields.uintegersNullableSize, 64)
			}
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				if hasSortable && typeName == "*uint64" {
					mapBindToRedisSearch[prefix+f.Name] = func(val interface{}) interface{} {
						if val == nil {
							return RedisSearchNullNumber
						} else if val.(uint64) > math.MaxInt32 {
							panic(errors.New("integer too high for redis search sort field"))
						}
						return val
					}
				} else {
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableNumeric
				}
			}
			mapBindToScanPointer[prefix+f.Name] = scanIntNullablePointer
			mapPointerToValue[prefix+f.Name] = pointerUintNullableScan
		case "int",
			"int8",
			"int16",
			"int32",
			"int64":
			fields.integers = append(fields.integers, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				if hasSortable && typeName == "int64" {
					mapBindToRedisSearch[prefix+f.Name] = func(val interface{}) interface{} {
						if val.(int64) > math.MaxInt32 {
							panic(errors.New("integer too high for redis search sort field"))
						}
						return val
					}
				} else {
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapper
				}
			}
			mapBindToScanPointer[prefix+f.Name] = func() interface{} {
				v := int64(0)
				return &v
			}
			mapPointerToValue[prefix+f.Name] = func(val interface{}) interface{} {
				return *val.(*int64)
			}
		case "*int",
			"*int8",
			"*int16",
			"*int32",
			"*int64":
			fields.integersNullable = append(fields.integersNullable, i)
			switch typeName {
			case "*int":
				fields.integersNullableSize = append(fields.integersNullableSize, 0)
			case "*int8":
				fields.integersNullableSize = append(fields.integersNullableSize, 8)
			case "*int16":
				fields.integersNullableSize = append(fields.integersNullableSize, 16)
			case "*int32":
				fields.integersNullableSize = append(fields.integersNullableSize, 32)
			case "*int64":
				fields.integersNullableSize = append(fields.integersNullableSize, 64)
			}
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				if hasSortable && typeName == "*int64" {
					mapBindToRedisSearch[prefix+f.Name] = func(val interface{}) interface{} {
						if val == nil {
							return RedisSearchNullNumber
						} else if val.(int64) > math.MaxInt32 {
							panic(errors.New("integer too high for redis search sort field"))
						}
						return val
					}
				} else {
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableNumeric
				}
			}
			mapBindToScanPointer[prefix+f.Name] = scanIntNullablePointer
			mapPointerToValue[prefix+f.Name] = pointerIntNullableScan
		case "string":
			enumCode, hasEnum := tags["enum"]
			if hasEnum {
				fields.stringsEnums = append(fields.stringsEnums, i)
				fields.enums = append(fields.enums, registry.enums[enumCode])
			} else {
				fields.strings = append(fields.strings, i)
			}
			if hasSearchable || hasSortable {
				if hasEnum {
					index.AddTagField(prefix+f.Name, hasSortable, !hasSearchable, ",")
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableString
				} else {
					stem, hasStem := tags["stem"]
					index.AddTextField(prefix+f.Name, 1.0, hasSortable, !hasSearchable, !hasStem || stem != "true")
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableString
				}
			}
			mapBindToScanPointer[prefix+f.Name] = func() interface{} {
				return &sql.NullString{}
			}
			mapPointerToValue[prefix+f.Name] = func(val interface{}) interface{} {
				v := val.(*sql.NullString)
				if v.Valid {
					return v.String
				}
				return nil
			}
		case "[]string":
			setCode, hasSet := tags["set"]
			if hasSet {
				fields.sliceStringsSets = append(fields.sliceStringsSets, i)
				fields.sets = append(fields.sets, registry.enums[setCode])
				if hasSearchable || hasSortable {
					index.AddTagField(prefix+f.Name, hasSortable, !hasSearchable, ",")
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableString
				}
			} else {
				fields.jsons = append(fields.jsons, i)
			}
			mapBindToScanPointer[prefix+f.Name] = scanStringNullablePointer
			mapPointerToValue[prefix+f.Name] = pointerStringNullableScan
		case "[]uint8":
			fields.bytes = append(fields.bytes, i)
		case "bool":
			if f.Name == "FakeDelete" {
				fields.fakeDelete = i
			} else {
				fields.booleans = append(fields.booleans, i)
				mapBindToScanPointer[prefix+f.Name] = scanBoolPointer
				mapPointerToValue[prefix+f.Name] = pointerBoolScan
				if hasSearchable || hasSortable {
					index.AddTagField(prefix+f.Name, hasSortable, !hasSearchable, ",")
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableBool
				}
			}
		case "*bool":
			fields.booleansNullable = append(fields.booleansNullable, i)
			if hasSearchable || hasSortable {
				index.AddTagField(prefix+f.Name, hasSortable, !hasSearchable, ",")
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableBool
			}
			mapBindToScanPointer[prefix+f.Name] = scanBoolNullablePointer
			mapPointerToValue[prefix+f.Name] = pointerBoolNullableScan
		case "float32",
			"float64":
			precision := 8
			if typeName == "float32" {
				precision = 4
			}
			precisionAttribute, has := tags["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			} else {
				decimal, has := tags["decimal"]
				if has {
					decimalArgs := strings.Split(decimal, ",")
					precision, _ = strconv.Atoi(decimalArgs[1])
				}
			}
			fields.floats = append(fields.floats, i)
			fields.floatsPrecision = append(fields.floatsPrecision, precision)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapper
			}
			mapBindToScanPointer[prefix+f.Name] = func() interface{} {
				v := float64(0)
				return &v
			}
			mapPointerToValue[prefix+f.Name] = func(val interface{}) interface{} {
				return *val.(*float64)
			}
		case "*float32",
			"*float64":
			precision := 8
			if typeName == "*float32" {
				precision = 4
				fields.floatsNullableSize = append(fields.floatsNullableSize, 32)
			} else {
				fields.floatsNullableSize = append(fields.floatsNullableSize, 64)
			}
			precisionAttribute, has := tags["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			} else {
				precisionAttribute, has := tags["decimal"]
				if has {
					precision, _ = strconv.Atoi(strings.Split(precisionAttribute, ",")[1])
				}
			}
			fields.floatsNullable = append(fields.floatsNullable, i)
			fields.floatsNullablePrecision = append(fields.floatsNullablePrecision, precision)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableNumeric
			}
			mapBindToScanPointer[prefix+f.Name] = scanFloatNullablePointer
			mapPointerToValue[prefix+f.Name] = pointerFloatNullableScan
		case "*time.Time":
			_, hasTime := tags["time"]
			if hasTime {
				fields.timesNullable = append(fields.timesNullable, i)
			} else {
				fields.datesNullable = append(fields.datesNullable, i)
			}
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableTime
			}
			mapBindToScanPointer[prefix+f.Name] = scanStringNullablePointer
			mapPointerToValue[prefix+f.Name] = pointerStringNullableScan
		case "*beeorm.CachedQuery":
			continue
		case "time.Time":
			_, hasTime := tags["time"]
			if hasTime {
				fields.times = append(fields.times, i)
			} else {
				fields.dates = append(fields.dates, i)
			}
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableTime
			}
			mapBindToScanPointer[prefix+f.Name] = scanStringPointer
			mapPointerToValue[prefix+f.Name] = pointerStringScan
		default:
			k := f.Type.Kind().String()
			if k == "struct" {
				fields.structs = append(fields.structs, i)
				subPrefix := ""
				if !f.Anonymous {
					subPrefix = f.Name
				}
				subFields := buildTableFields(f.Type, registry, index, mapBindToRedisSearch,
					mapBindToScanPointer, mapPointerToValue, 0, subPrefix, schemaTags)
				fields.structsFields = append(fields.structsFields, subFields)
			} else if k == "ptr" {
				modelType := reflect.TypeOf((*Entity)(nil)).Elem()
				if f.Type.Implements(modelType) {
					fields.refs = append(fields.refs, i)
					fields.refsTypes = append(fields.refsTypes, f.Type.Elem())
					if hasSearchable || hasSortable {
						index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
						mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableNumeric
					}
					mapBindToScanPointer[prefix+f.Name] = scanIntNullablePointer
					mapPointerToValue[prefix+f.Name] = pointerUintNullableScan
				} else {
					fields.jsons = append(fields.jsons, i)
				}
			} else {
				if typeName[0:3] == "[]*" {
					modelType := reflect.TypeOf((*Entity)(nil)).Elem()
					t := f.Type.Elem()
					if t.Implements(modelType) {
						fields.refsMany = append(fields.refsMany, i)
						fields.refsManyTypes = append(fields.refsManyTypes, t.Elem())
						continue
					}
				}
				fields.jsons = append(fields.jsons, i)
			}
		}
	}
	return fields
}

func extractTags(registry *Registry, entityType reflect.Type, prefix string) (fields map[string]map[string]string) {
	fields = make(map[string]map[string]string)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		for k, v := range extractTag(registry, field) {
			fields[prefix+k] = v
		}
		_, hasIgnore := fields[field.Name]["ignore"]
		if hasIgnore {
			continue
		}
		refOne := ""
		refMany := ""
		hasRef := false
		hasRefMany := false
		if field.Type.Kind().String() == "ptr" {
			refName := field.Type.Elem().String()
			_, hasRef = registry.entities[refName]
			if hasRef {
				refOne = refName
			}
		} else if field.Type.String()[0:3] == "[]*" {
			refName := field.Type.String()[3:]
			_, hasRefMany = registry.entities[refName]
			if hasRefMany {
				refMany = refName
			}
		}

		query, hasQuery := field.Tag.Lookup("query")
		queryOne, hasQueryOne := field.Tag.Lookup("queryOne")
		if hasQuery {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["query"] = query
		}
		if hasQueryOne {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["queryOne"] = queryOne
		}
		if hasRef {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["ref"] = refOne
		}
		if hasRefMany {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["refs"] = refMany
		}
	}
	return
}

func extractTag(registry *Registry, field reflect.StructField) map[string]map[string]string {
	tag, ok := field.Tag.Lookup("orm")
	if ok {
		args := strings.Split(tag, ";")
		length := len(args)
		var attributes = make(map[string]string, length)
		for j := 0; j < length; j++ {
			arg := strings.Split(args[j], "=")
			if len(arg) == 1 {
				attributes[arg[0]] = "true"
			} else {
				attributes[arg[0]] = arg[1]
			}
		}
		return map[string]map[string]string{field.Name: attributes}
	} else if field.Type.Kind().String() == "struct" {
		t := field.Type.String()
		if t != "beeorm.ORM" && t != "time.Time" {
			prefix := ""
			if !field.Anonymous {
				prefix = field.Name
			}
			return extractTags(registry, field.Type, prefix)
		}
	}
	return make(map[string]map[string]string)
}

func (tableSchema *tableSchema) getCacheKey(id uint64) string {
	return tableSchema.cachePrefix + ":" + strconv.FormatUint(id, 10)
}

func (tableSchema *tableSchema) NewEntity() Entity {
	val := reflect.New(tableSchema.t)
	e := val.Interface().(Entity)
	orm := e.getORM()
	orm.initialised = true
	orm.tableSchema = tableSchema
	orm.value = val
	orm.elem = val.Elem()
	orm.idElem = orm.elem.Field(1)
	return e
}

func (fields *tableFields) getColumnNames() ([]string, string) {
	fieldsQuery := ""
	columns := make([]string, 0)
	ids := fields.refs
	ids = append(ids, fields.uintegers...)
	ids = append(ids, fields.integers...)
	ids = append(ids, fields.booleans...)
	ids = append(ids, fields.floats...)
	timesStart := len(ids)
	ids = append(ids, fields.times...)
	ids = append(ids, fields.dates...)
	timesEnd := len(ids)
	if fields.fakeDelete > 0 {
		ids = append(ids, fields.fakeDelete)
	}
	ids = append(ids, fields.strings...)
	ids = append(ids, fields.uintegersNullable...)
	ids = append(ids, fields.integersNullable...)
	ids = append(ids, fields.stringsEnums...)
	ids = append(ids, fields.bytes...)
	ids = append(ids, fields.sliceStringsSets...)
	ids = append(ids, fields.booleansNullable...)
	ids = append(ids, fields.floatsNullable...)
	timesNullableStart := len(ids)
	ids = append(ids, fields.timesNullable...)
	ids = append(ids, fields.datesNullable...)
	timesNullableEnd := len(ids)
	ids = append(ids, fields.jsons...)
	ids = append(ids, fields.refsMany...)
	for k, i := range ids {
		name := fields.prefix + fields.fields[i].Name
		columns = append(columns, name)
		if (k >= timesStart && k < timesEnd) || (k >= timesNullableStart && k < timesNullableEnd) {
			fieldsQuery += ",UNIX_TIMESTAMP(`" + name + "`)"
		} else {
			fieldsQuery += ",`" + name + "`"
		}
	}
	for _, subFields := range fields.structsFields {
		subColumns, subQuery := subFields.getColumnNames()
		columns = append(columns, subColumns...)
		fieldsQuery += "," + subQuery
	}
	return columns, fieldsQuery[1:]
}

var defaultRedisSearchMapper = func(val interface{}) interface{} {
	return val
}

var defaultRedisSearchMapperNullableString = func(val interface{}) interface{} {
	if val == nil {
		return "NULL"
	}
	return EscapeRedisSearchString(val.(string))
}

var defaultRedisSearchMapperNullableNumeric = func(val interface{}) interface{} {
	if val == nil {
		return RedisSearchNullNumber
	}
	return val
}

var defaultRedisSearchMapperNullableBool = func(val interface{}) interface{} {
	if val == nil {
		return "NULL"
	}
	if val.(bool) {
		return "true"
	}
	return "false"
}

var defaultRedisSearchMapperNullableTime = func(val interface{}) interface{} {
	if val == nil {
		return RedisSearchNullNumber
	}
	v := val.(string)
	if v[0:10] == "0001-01-01" {
		return 0
	}
	if len(v) == 19 {
		t, _ := time.ParseInLocation(timeFormat, v, time.Local)
		return t.Unix()
	}
	t, _ := time.ParseInLocation(dateformat, v, time.Local)
	return t.Unix()
}

var scanIntNullablePointer = func() interface{} {
	return &sql.NullInt64{}
}

var pointerUintNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return uint64(v.Int64)
	}
	return nil
}

var pointerIntNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return v.Int64
	}
	return nil
}

var scanStringNullablePointer = func() interface{} {
	return &sql.NullString{}
}

var pointerStringNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullString)
	if v.Valid {
		return v.String
	}
	return nil
}

var scanBoolPointer = func() interface{} {
	v := false
	return &v
}

var pointerBoolScan = func(val interface{}) interface{} {
	return *val.(*bool)
}

var scanBoolNullablePointer = func() interface{} {
	return &sql.NullBool{}
}

var pointerBoolNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullBool)
	if v.Valid {
		return v.Bool
	}
	return nil
}

var scanFloatNullablePointer = func() interface{} {
	return &sql.NullFloat64{}
}

var pointerFloatNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullFloat64)
	if v.Valid {
		return v.Float64
	}
	return nil
}

var scanStringPointer = func() interface{} {
	v := ""
	return &v
}

var pointerStringScan = func(val interface{}) interface{} {
	return *val.(*string)
}
