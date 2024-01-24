package beeorm

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/puzpuzpuz/xsync/v2"
)

var codeStartTime = uint64(time.Now().Unix())

func GetEntitySchema[E any](orm ORM) EntitySchema {
	return getEntitySchema[E](orm)
}

func getEntitySchema[E any](orm ORM) *entitySchema {
	var entity E
	return getEntitySchemaFromSource(orm, entity)
}

func getEntitySchemaFromSource(orm ORM, source any) *entitySchema {
	ci := orm.(*ormImplementation)
	schema, has := ci.engine.registry.entitySchemasQuickMap[reflect.TypeOf(source)]
	if !has {
		panic(fmt.Errorf("entity '%T' is not registered", source))
	}
	return schema
}

type EntitySchemaSetter interface {
	SetOption(key string, value any)
	EntitySchemaShared
}

type EntitySchemaShared interface {
	GetTableName() string
	GetType() reflect.Type
	GetColumns() []string
	GetTag(field, key, trueValue, defaultValue string) string
	Option(key string) any
	GetUniqueIndexes() map[string][]string
	GetDB() DB
	GetLocalCache() (cache LocalCache, has bool)
	GetRedisCache() (cache RedisCache, has bool)
}

type EntitySchema interface {
	EntitySchemaShared
	DropTable(orm ORM)
	TruncateTable(orm ORM)
	UpdateSchema(orm ORM)
	UpdateSchemaAndTruncateTable(orm ORM)
	GetSchemaChanges(orm ORM) (alters []Alter, has bool)
	DisableCache(local, redis bool)
	NewEntity(orm ORM) any
	GetByID(orm ORM, id uint64) (entity any, found bool)
	Search(orm ORM, where Where, pager *Pager) EntityAnonymousIterator
	SearchWithCount(orm ORM, where Where, pager *Pager) (results EntityAnonymousIterator, totalRows int)
	SearchIDs(orm ORM, where Where, pager *Pager) []uint64
	SearchIDsWithCount(orm ORM, where Where, pager *Pager) (results []uint64, totalRows int)
	IsDirty(orm ORM, id uint64) (oldValues, newValues Bind, hasChanges bool)
	Copy(orm ORM, source any) any
	EditEntityField(orm ORM, entity any, field string, value any) error
	EditEntity(orm ORM, entity any) any
	DeleteEntity(orm ORM, entity any)
	getCacheKey() string
	uuid(orm ORM) uint64
	getForcedRedisCode() string
}

type columnAttrToStringSetter func(v any, fromBind bool) (string, error)
type fieldBindSetter func(v any) (any, error)
type fieldSetter func(v any, e reflect.Value)
type fieldGetter func(reflect.Value) any

type entitySchema struct {
	index                     uint64
	tableName                 string
	archived                  bool
	mysqlPoolCode             string
	t                         reflect.Type
	tSlice                    reflect.Type
	fields                    *tableFields
	engine                    Engine
	fieldsQuery               string
	tags                      map[string]map[string]string
	columnNames               []string
	columnMapping             map[string]int
	columnAttrToStringSetters map[string]columnAttrToStringSetter
	fieldBindSetters          map[string]fieldBindSetter
	fieldSetters              map[string]fieldSetter
	fieldGetters              map[string]fieldGetter
	uniqueIndices             map[string][]string
	references                map[string]referenceDefinition
	cachedReferences          map[string]referenceDefinition
	indexes                   map[string]indexDefinition
	cachedIndexes             map[string]indexDefinition
	options                   map[string]any
	cacheAll                  bool
	hasLocalCache             bool
	localCache                *localCache
	localCacheLimit           int
	redisCacheName            string
	hasRedisCache             bool
	redisCache                *redisCache
	cacheKey                  string
	uuidCacheKey              string
	uuidMutex                 sync.Mutex
	asyncCacheKey             string
	structureHash             string
	mapBindToScanPointer      mapBindToScanPointer
	mapPointerToValue         mapPointerToValue
	asyncTemporaryQueue       *xsync.MPMCQueueOf[asyncTemporaryQueueEvent]
}

type mapBindToScanPointer map[string]func() any
type mapPointerToValue map[string]func(val any) any

type tableFields struct {
	t                              reflect.Type
	fields                         map[int]reflect.StructField
	forcedOldBid                   map[int]bool
	arrays                         map[int]int
	prefix                         string
	uIntegers                      []int
	uIntegersArray                 []int
	integers                       []int
	integersArray                  []int
	references                     []int
	referencesArray                []int
	referencesRequired             []bool
	referencesRequiredArray        []bool
	uIntegersNullable              []int
	uIntegersNullableArray         []int
	uIntegersNullableSize          []int
	uIntegersNullableSizeArray     []int
	integersNullable               []int
	integersNullableArray          []int
	integersNullableSize           []int
	integersNullableSizeArray      []int
	strings                        []int
	stringsArray                   []int
	stringMaxLengths               []int
	stringMaxLengthsArray          []int
	stringsRequired                []bool
	stringsRequiredArray           []bool
	stringsEnums                   []int
	stringsEnumsArray              []int
	enums                          []*enumDefinition
	enumsArray                     []*enumDefinition
	sliceStringsSets               []int
	sliceStringsSetsArray          []int
	sets                           []*enumDefinition
	setsArray                      []*enumDefinition
	bytes                          []int
	bytesArray                     []int
	booleans                       []int
	booleansArray                  []int
	booleansNullable               []int
	booleansNullableArray          []int
	floats                         []int
	floatsArray                    []int
	floatsPrecision                []int
	floatsPrecisionArray           []int
	floatsDecimalSize              []int
	floatsDecimalSizeArray         []int
	floatsSize                     []int
	floatsSizeArray                []int
	floatsUnsigned                 []bool
	floatsUnsignedArray            []bool
	floatsNullable                 []int
	floatsNullableArray            []int
	floatsNullablePrecision        []int
	floatsNullablePrecisionArray   []int
	floatsNullableDecimalSize      []int
	floatsNullableDecimalSizeArray []int
	floatsNullableUnsigned         []bool
	floatsNullableUnsignedArray    []bool
	floatsNullableSize             []int
	floatsNullableSizeArray        []int
	timesNullable                  []int
	timesNullableArray             []int
	datesNullable                  []int
	datesNullableArray             []int
	times                          []int
	timesArray                     []int
	dates                          []int
	datesArray                     []int
	structs                        []int
	structsArray                   []int
	structsFields                  []*tableFields
	structsFieldsArray             []*tableFields
}

func (e *entitySchema) GetTableName() string {
	return e.tableName
}

func (e *entitySchema) GetType() reflect.Type {
	return e.t
}

func (e *entitySchema) DropTable(orm ORM) {
	pool := e.GetDB()
	pool.Exec(orm, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetConfig().GetDatabaseName(), e.tableName))
}

func (e *entitySchema) TruncateTable(orm ORM) {
	pool := e.GetDB()
	if e.archived {
		_ = pool.Exec(orm, fmt.Sprintf("DROP TABLE `%s`.`%s`", pool.GetConfig().GetDatabaseName(), e.tableName))
		e.UpdateSchema(orm)
	} else {
		_ = pool.Exec(orm, fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", pool.GetConfig().GetDatabaseName(), e.tableName))
	}
}

func (e *entitySchema) UpdateSchema(orm ORM) {
	pool := e.GetDB()
	alters, has := e.GetSchemaChanges(orm)
	if has {
		for _, alter := range alters {
			_ = pool.Exec(orm, alter.SQL)
		}
	}
}

func (e *entitySchema) UpdateSchemaAndTruncateTable(orm ORM) {
	e.UpdateSchema(orm)
	pool := e.GetDB()
	_ = pool.Exec(orm, fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetConfig().GetDatabaseName(), e.tableName))
	_ = pool.Exec(orm, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetConfig().GetDatabaseName(), e.tableName))
}

func (e *entitySchema) GetDB() DB {
	return e.engine.DB(e.mysqlPoolCode)
}

func (e *entitySchema) GetLocalCache() (cache LocalCache, has bool) {
	if !e.hasLocalCache {
		return nil, false
	}
	return e.localCache, true
}

func (e *entitySchema) GetRedisCache() (cache RedisCache, has bool) {
	if !e.hasRedisCache {
		return nil, false
	}
	return e.redisCache, true
}

func (e *entitySchema) GetColumns() []string {
	return e.columnNames
}

func (e *entitySchema) GetUniqueIndexes() map[string][]string {
	return e.uniqueIndices
}

func (e *entitySchema) GetSchemaChanges(orm ORM) (alters []Alter, has bool) {
	pre, alters, post := getSchemaChanges(orm, e)
	final := pre
	final = append(final, alters...)
	final = append(final, post...)
	return final, len(final) > 0
}

func (e *entitySchema) init(registry *registry, entityType reflect.Type) error {
	e.t = entityType
	e.tSlice = reflect.SliceOf(reflect.PtrTo(entityType))
	e.tags = extractTags(registry, entityType, "")
	e.options = make(map[string]any)
	e.references = make(map[string]referenceDefinition)
	e.cachedReferences = make(map[string]referenceDefinition)
	e.indexes = make(map[string]indexDefinition)
	e.cachedIndexes = make(map[string]indexDefinition)
	e.mapBindToScanPointer = mapBindToScanPointer{}
	e.mapPointerToValue = mapPointerToValue{}
	e.mysqlPoolCode = e.getTag("mysql", "default", DefaultPoolCode)
	_, has := registry.mysqlPools[e.mysqlPoolCode]
	if !has {
		return fmt.Errorf("mysql pool '%s' not found", e.mysqlPoolCode)
	}
	e.tableName = e.getTag("table", entityType.Name(), entityType.Name())
	e.archived = e.getTag("archived", "true", "") == "true"
	e.cacheAll = e.getTag("cacheAll", "true", "") == "true"
	redisCacheName := e.getTag("redisCache", DefaultPoolCode, "")
	if redisCacheName != "" {
		_, has = registry.redisPools[redisCacheName]
		if !has {
			return fmt.Errorf("redis pool '%s' not found", redisCacheName)
		}
	}
	cacheKey := ""
	if e.mysqlPoolCode != DefaultPoolCode {
		cacheKey = e.mysqlPoolCode
	}
	cacheKey += e.tableName
	uniqueIndices := make(map[string]map[int]string)
	indices := make(map[string]map[int]string)
	uniqueGlobal := e.getTag("unique", "", "")
	if uniqueGlobal != "" {
		parts := strings.Split(uniqueGlobal, "|")
		for _, part := range parts {
			def := strings.Split(part, ":")
			uniqueIndices[def[0]] = make(map[int]string)
			for i, field := range strings.Split(def[1], ",") {
				uniqueIndices[def[0]][i+1] = field
			}
		}
	}
	for k, v := range e.tags {
		keys, has := v["unique"]
		if has {
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
	}
	e.columnAttrToStringSetters = make(map[string]columnAttrToStringSetter)
	e.fieldBindSetters = make(map[string]fieldBindSetter)
	e.fieldSetters = make(map[string]fieldSetter)
	e.fieldGetters = make(map[string]fieldGetter)
	e.fields = e.buildTableFields(entityType, registry, 0, "", nil, e.tags)
	e.columnNames, e.fieldsQuery = e.fields.buildColumnNames("")
	if len(e.fieldsQuery) > 0 {
		e.fieldsQuery = e.fieldsQuery[1:]
	}
	columnMapping := make(map[string]int)
	for i, name := range e.columnNames {
		columnMapping[name] = i
	}
	cacheKey = hashString(cacheKey + e.fieldsQuery)
	e.uuidCacheKey = cacheKey[0:12]
	cacheKey = cacheKey[0:5]
	h := fnv.New32a()
	_, _ = h.Write([]byte(cacheKey))

	e.structureHash = strconv.FormatUint(uint64(h.Sum32()), 10)
	e.columnMapping = columnMapping
	localCacheLimit := e.getTag("localCache", "0", "")
	if localCacheLimit != "" {
		localCacheLimitAsInt, err := strconv.Atoi(localCacheLimit)
		if err != nil {
			return fmt.Errorf("invalid local cache pool limit '%s'", localCacheLimit)
		}
		e.hasLocalCache = true
		e.localCacheLimit = localCacheLimitAsInt
	}
	e.redisCacheName = redisCacheName
	e.hasRedisCache = redisCacheName != ""
	e.cacheKey = cacheKey
	e.asyncCacheKey = flushAsyncEventsList
	asyncGroup := e.getTag("split_async_flush", "true", "")
	if asyncGroup == "true" {
		e.asyncCacheKey += ":" + e.cacheKey
	} else if asyncGroup != "" {
		e.asyncCacheKey = asyncGroup
	}
	e.asyncTemporaryQueue = xsync.NewMPMCQueueOf[asyncTemporaryQueueEvent](10000)
	e.uniqueIndices = make(map[string][]string)
	for name, index := range uniqueIndices {
		e.uniqueIndices[name] = make([]string, len(index))
		for i := 1; i <= len(index); i++ {
			e.uniqueIndices[name][i-1] = index[i]
		}
	}
	err := e.validateIndexes(uniqueIndices, indices)
	if err != nil {
		return err
	}
	for indexName, indexColumns := range indices {
		where := ""
		for i := 0; i < len(indexColumns); i++ {
			if i > 0 {
				where += " AND "
			}
			where += "`" + indexColumns[i+1] + "`=?"
		}
		cached := false
		tags, hasTag := e.tags[indexColumns[1]]
		if hasTag {
			cached = tags["cached"] == "true"
		}
		columnsList := make([]string, len(indexColumns))
		for j := 0; j < len(indexColumns); j++ {
			columnsList[j] = indexColumns[j+1]
		}

		definition := indexDefinition{Where: where, Cached: cached, Columns: columnsList}
		e.indexes[indexName] = definition
		if cached {
			e.cachedIndexes[indexName] = definition
		}
	}
	for _, plugin := range registry.plugins {
		pluginInterfaceValidateEntitySchema, isInterface := plugin.(PluginInterfaceValidateEntitySchema)
		if isInterface {
			err = pluginInterfaceValidateEntitySchema.ValidateEntitySchema(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *entitySchema) validateIndexes(uniqueIndices map[string]map[int]string, indices map[string]map[int]string) error {
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
				return fmt.Errorf("duplicated index %s with %s in %s", k, k2, e.t.String())
			}
		}
	}
	return nil
}

func (e *entitySchema) getTag(key, trueValue, defaultValue string) string {
	userValue, has := e.tags["ID"][key]
	if has {
		if userValue == "true" {
			return trueValue
		}
		return userValue
	}
	return e.GetTag("ID", key, trueValue, defaultValue)
}

func (e *entitySchema) GetTag(field, key, trueValue, defaultValue string) string {
	userValue, has := e.tags[field][key]
	if has {
		if userValue == "true" {
			return trueValue
		}
		return userValue
	}
	return defaultValue
}

func (e *entitySchema) SetOption(key string, value any) {
	e.options[key] = value
}

func (e *entitySchema) Option(key string) any {
	return e.options[key]
}

func (e *entitySchema) uuid(orm ORM) uint64 {
	r := orm.Engine().Redis(e.getForcedRedisCode())
	id := r.Incr(orm, e.uuidCacheKey)
	if id == 1 {
		e.initUUID(orm)
		return e.uuid(orm)
	}
	return uint64(id)
}

func (e *entitySchema) initUUID(orm ORM) {
	r := orm.Engine().Redis(e.getForcedRedisCode())
	e.uuidMutex.Lock()
	defer e.uuidMutex.Unlock()
	now, has := r.Get(orm, e.uuidCacheKey)
	if has && now != "1" {
		return
	}
	lockName := e.uuidCacheKey + ":lock"
	lock, obtained := r.GetLocker().Obtain(orm, lockName, time.Minute, time.Second*5)
	if !obtained {
		panic(errors.New("uuid lock timeout"))
	}
	defer lock.Release(orm)
	now, has = r.Get(orm, e.uuidCacheKey)
	if has && now != "1" {
		return
	}
	maxID := int64(0)
	e.GetDB().QueryRow(orm, NewWhere("SELECT IFNULL(MAX(ID), 0) FROM `"+e.GetTableName()+"`"), &maxID)
	if maxID == 0 {
		maxID = 1
	}
	r.IncrBy(orm, e.uuidCacheKey, maxID)
}

func (e *entitySchema) getForcedRedisCode() string {
	if e.hasRedisCache {
		return e.redisCacheName
	}
	return DefaultPoolCode
}

func (e *entitySchema) getCacheKey() string {
	return e.cacheKey
}

func (e *entitySchema) DisableCache(local, redis bool) {
	if local {
		e.hasLocalCache = false
	}
	if redis {
		e.redisCacheName = ""
		e.hasRedisCache = false
	}
}

func (e *entitySchema) NewEntity(orm ORM) any {
	return newEntity(orm, orm.Engine().Registry().EntitySchema(e.t).(*entitySchema))
}

func (e *entitySchema) GetByID(orm ORM, id uint64) (entity any, found bool) {
	return getByID(orm.(*ormImplementation), id, orm.Engine().Registry().EntitySchema(e.t).(*entitySchema))
}

func (e *entitySchema) SearchWithCount(orm ORM, where Where, pager *Pager) (results EntityAnonymousIterator, totalRows int) {
	return e.search(orm, where, pager, true)
}

func (e *entitySchema) Search(orm ORM, where Where, pager *Pager) EntityAnonymousIterator {
	results, _ := e.search(orm, where, pager, false)
	return results
}

func (e *entitySchema) SearchIDs(orm ORM, where Where, pager *Pager) []uint64 {
	schema := orm.Engine().Registry().EntitySchema(e.t).(*entitySchema)
	ids, _ := searchIDs(orm, schema, where, pager, false)
	return ids
}

func (e *entitySchema) SearchIDsWithCount(orm ORM, where Where, pager *Pager) (results []uint64, totalRows int) {
	schema := orm.Engine().Registry().EntitySchema(e.t).(*entitySchema)
	return searchIDs(orm, schema, where, pager, true)
}

func (e *entitySchema) IsDirty(orm ORM, id uint64) (oldValues, newValues Bind, hasChanges bool) {
	return isDirty(orm, orm.Engine().Registry().EntitySchema(e.t).(*entitySchema), id)
}

func (e *entitySchema) Copy(orm ORM, source any) any {
	schema := orm.Engine().Registry().EntitySchema(e.t).(*entitySchema)
	insertable := newEntityInsertable(orm, schema)
	copyEntity(reflect.ValueOf(source).Elem(), insertable.value.Elem(), schema.fields, false)
	return insertable.entity
}

func (e *entitySchema) EditEntityField(orm ORM, entity any, field string, value any) error {
	return editEntityField(orm, entity, field, value)
}

func (e *entitySchema) EditEntity(orm ORM, source any) any {
	writable := copyToEdit(orm, source)
	writable.id = writable.value.Elem().Field(0).Uint()
	writable.source = source
	orm.trackEntity(writable)
	return writable.entity
}

func (e *entitySchema) DeleteEntity(orm ORM, source any) {
	schema := orm.Engine().Registry().EntitySchema(e.t).(*entitySchema)
	toRemove := &removableEntity{}
	toRemove.orm = orm
	toRemove.source = source
	toRemove.value = reflect.ValueOf(source).Elem()
	toRemove.id = toRemove.value.Field(0).Uint()
	toRemove.schema = schema
	orm.trackEntity(toRemove)
}

func (e *entitySchema) search(orm ORM, where Where, pager *Pager, withCount bool) (results EntityAnonymousIterator, totalRows int) {
	schema := orm.Engine().Registry().EntitySchema(e.t).(*entitySchema)
	entities := reflect.New(reflect.SliceOf(reflect.PtrTo(e.t))).Elem()
	if schema.hasLocalCache {
		ids, total := searchIDs(orm, schema, where, pager, withCount)
		if total == 0 {
			return emptyResultsAnonymousIteratorInstance, 0
		}
		return &localCacheIDsAnonymousIterator{c: orm.(*ormImplementation), schema: schema, ids: ids, index: -1}, total
	}
	whereQuery := where.String()
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE " + whereQuery
	if pager != nil {
		query += " " + pager.String()
	}
	pool := schema.GetDB()
	queryResults, def := pool.Query(orm, query, where.GetParameters()...)
	defer def()

	i := 0
	for queryResults.Next() {
		pointers := prepareScan(schema)
		queryResults.Scan(pointers...)
		value := reflect.New(schema.t)
		deserializeFromDB(schema.fields, value.Elem(), pointers)
		entities = reflect.Append(entities, value)
		i++
	}
	def()
	totalRows = i
	if pager != nil {
		totalRows = getTotalRows(orm, withCount, pager, where, schema, i)
	}
	resultsIterator := &entityAnonymousIterator{index: -1}
	resultsIterator.rows = entities
	return resultsIterator, totalRows
}

func (e *entitySchema) buildTableFields(t reflect.Type, registry *registry,
	start int, prefix string, parents []int, schemaTags map[string]map[string]string) *tableFields {
	fields := &tableFields{t: t, prefix: prefix, fields: make(map[int]reflect.StructField)}
	fields.forcedOldBid = make(map[int]bool)
	fields.arrays = make(map[int]int)
	for i := start; i < t.NumField(); i++ {
		f := t.Field(i)
		tags := schemaTags[prefix+f.Name]
		_, has := tags["ignore"]
		if has {
			continue
		}
		_, has = tags["unique"]
		if has {
			fields.forcedOldBid[i] = true
		}
		attributes := schemaFieldAttributes{
			Fields:   fields,
			Tags:     tags,
			Index:    i,
			Parents:  parents,
			Prefix:   prefix,
			Field:    f,
			TypeName: f.Type.String(),
		}
		fields.fields[i] = f
		if f.Type.Kind().String() == "array" {
			attributes.TypeName = f.Type.Elem().String()
			fields.arrays[i] = f.Type.Len()
			attributes.IsArray = true
		}

		switch attributes.TypeName {
		case "uint":
			e.buildUintField(attributes, 0, math.MaxUint)
		case "uint8":
			e.buildUintField(attributes, 0, math.MaxUint8)
		case "uint16":
			e.buildUintField(attributes, 0, math.MaxUint16)
		case "uint32":
			e.buildUintField(attributes, 0, math.MaxUint32)
		case "uint64":
			e.buildUintField(attributes, 0, math.MaxUint64)
		case "*uint":
			e.buildUintPointerField(attributes, 0, math.MaxUint)
		case "*uint8":
			e.buildUintPointerField(attributes, 0, math.MaxUint8)
		case "*uint16":
			e.buildUintPointerField(attributes, 0, math.MaxUint16)
		case "*uint32":
			e.buildUintPointerField(attributes, 0, math.MaxUint32)
		case "*uint64":
			e.buildUintPointerField(attributes, 0, math.MaxUint64)
		case "int":
			e.buildIntField(attributes, math.MinInt, math.MaxInt)
		case "int8":
			e.buildIntField(attributes, math.MinInt8, math.MaxInt8)
		case "int16":
			e.buildIntField(attributes, math.MinInt16, math.MaxInt16)
		case "int32":
			e.buildIntField(attributes, math.MinInt32, math.MaxInt32)
		case "int64":
			e.buildIntField(attributes, math.MinInt64, math.MaxInt64)

		case "*int":
			e.buildIntPointerField(attributes, math.MinInt, math.MaxInt)
		case "*int8":
			e.buildIntPointerField(attributes, math.MinInt8, math.MaxInt8)
		case "*int16":
			e.buildIntPointerField(attributes, math.MinInt16, math.MaxInt16)
		case "*int32":
			e.buildIntPointerField(attributes, math.MinInt32, math.MaxInt32)
		case "*int64":
			e.buildIntPointerField(attributes, math.MinInt64, math.MaxInt64)
		case "string":
			e.buildStringField(attributes)
		case "[]uint8":
			e.buildBytesField(attributes)
		case "bool":
			e.buildBoolField(attributes)
		case "*bool":
			e.buildBoolPointerField(attributes)
		case "float32",
			"float64":
			e.buildFloatField(attributes)
		case "*float32",
			"*float64":
			e.buildFloatPointerField(attributes)
		case "*time.Time":
			e.buildTimePointerField(attributes)
		case "time.Time":
			e.buildTimeField(attributes)
		default:
			fType := f.Type
			if attributes.IsArray {
				fType = fType.Elem()
			}
			k := fType.Kind().String()
			if k == "struct" {
				e.buildStructField(attributes, registry, schemaTags)
			} else if fType.Implements(reflect.TypeOf((*EnumValues)(nil)).Elem()) {
				definition := reflect.New(fType).Interface().(EnumValues).EnumValues()
				e.buildEnumField(attributes, fType.String(), definition)
			} else if k == "slice" && fType.Elem().Implements(reflect.TypeOf((*EnumValues)(nil)).Elem()) {
				definition := reflect.New(fType.Elem()).Interface().(EnumValues).EnumValues()
				e.buildStringSliceField(fType.String(), attributes, definition)
			} else if fType.Implements(reflect.TypeOf((*referenceInterface)(nil)).Elem()) {
				e.buildReferenceField(attributes)
				if attributes.Tags["cached"] == "true" {
					fields.forcedOldBid[i] = true
				}
			} else {
				panic(fmt.Errorf("%s field %s type %s is not supported", e.t.String(), f.Name, f.Type.String()))
			}
		}
	}
	return fields
}

type schemaFieldAttributes struct {
	Field    reflect.StructField
	TypeName string
	Tags     map[string]string
	Fields   *tableFields
	Index    int
	Parents  []int
	Prefix   string
	IsArray  bool
}

func (attributes schemaFieldAttributes) GetColumnNames() []string {
	l, isArray := attributes.Fields.arrays[attributes.Index]
	if !isArray {
		return []string{attributes.Prefix + attributes.Field.Name}
	}
	names := make([]string, l)
	for i := 0; i <= l; i++ {
		if i == l {
			break
		}
		names[i] = attributes.Prefix + attributes.Field.Name + "_" + strconv.Itoa(i+1)
	}
	return names
}

func (e *entitySchema) buildUintField(attributes schemaFieldAttributes, min int64, max uint64) {
	if attributes.IsArray {
		attributes.Fields.uIntegersArray = append(attributes.Fields.uIntegersArray, attributes.Index)
	} else {
		attributes.Fields.uIntegers = append(attributes.Fields.uIntegers, attributes.Index)
	}

	for _, columnName := range attributes.GetColumnNames() {
		e.mapBindToScanPointer[columnName] = func() any {
			v := uint64(0)
			return &v
		}
		e.mapPointerToValue[columnName] = func(val any) any {
			return *val.(*uint64)
		}
		e.fieldBindSetters[columnName] = createNumberFieldBindSetter(columnName, true, false, min, max)
		e.columnAttrToStringSetters[columnName] = createUint64AttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createNumberFieldSetter(attributes, true, false)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildReferenceField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.referencesArray = append(attributes.Fields.referencesArray, attributes.Index)
	} else {
		attributes.Fields.references = append(attributes.Fields.references, attributes.Index)
	}
	fType := attributes.Field.Type
	if attributes.IsArray {
		fType = fType.Elem()
	}
	for i, columnName := range attributes.GetColumnNames() {
		isRequired := attributes.Tags["required"] == "true"
		if attributes.IsArray {
			attributes.Fields.referencesRequiredArray = append(attributes.Fields.referencesRequiredArray, isRequired)
		} else {
			attributes.Fields.referencesRequired = append(attributes.Fields.referencesRequired, isRequired)
		}

		e.mapBindToScanPointer[columnName] = scanIntNullablePointer
		e.mapPointerToValue[columnName] = pointerUintNullableScan
		var refType reflect.Type
		if i == 0 {
			refType = reflect.New(fType).Interface().(referenceInterface).getType()
			def := referenceDefinition{
				Cached: attributes.Tags["cached"] == "true",
				Type:   refType,
			}
			if def.Cached {
				e.cachedReferences[columnName] = def
			}
			e.references[columnName] = def
		}
		idSetter := createNumberFieldBindSetter(columnName, true, !isRequired, 0, math.MaxUint64)
		e.fieldBindSetters[columnName] = createReferenceFieldBindSetter(columnName, refType, idSetter, !isRequired)
		e.columnAttrToStringSetters[columnName] = createUint64AttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createReferenceFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildUintPointerField(attributes schemaFieldAttributes, min int64, max uint64) {
	if attributes.IsArray {
		attributes.Fields.uIntegersNullableArray = append(attributes.Fields.uIntegersNullableArray, attributes.Index)
	} else {
		attributes.Fields.uIntegersNullable = append(attributes.Fields.uIntegersNullable, attributes.Index)
	}
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			if attributes.IsArray {
				switch attributes.TypeName {
				case "*uint":
					attributes.Fields.uIntegersNullableSizeArray = append(attributes.Fields.uIntegersNullableSizeArray, 0)
				case "*uint8":
					attributes.Fields.uIntegersNullableSizeArray = append(attributes.Fields.uIntegersNullableSizeArray, 8)
				case "*uint16":
					attributes.Fields.uIntegersNullableSizeArray = append(attributes.Fields.uIntegersNullableSizeArray, 16)
				case "*uint32":
					attributes.Fields.uIntegersNullableSizeArray = append(attributes.Fields.uIntegersNullableSizeArray, 32)
				case "*uint64":
					attributes.Fields.uIntegersNullableSizeArray = append(attributes.Fields.uIntegersNullableSizeArray, 64)
				}
			} else {
				switch attributes.TypeName {
				case "*uint":
					attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 0)
				case "*uint8":
					attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 8)
				case "*uint16":
					attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 16)
				case "*uint32":
					attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 32)
				case "*uint64":
					attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 64)
				}
			}
		}
		e.mapBindToScanPointer[columnName] = scanIntNullablePointer
		e.mapPointerToValue[columnName] = pointerUintNullableScan
		e.fieldBindSetters[columnName] = createNumberFieldBindSetter(columnName, true, true, min, max)
		e.columnAttrToStringSetters[columnName] = createUint64AttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createNumberFieldSetter(attributes, true, true)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildIntField(attributes schemaFieldAttributes, min int64, max uint64) {
	if attributes.IsArray {
		attributes.Fields.integersArray = append(attributes.Fields.integersArray, attributes.Index)
	} else {
		attributes.Fields.integers = append(attributes.Fields.integers, attributes.Index)
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.mapBindToScanPointer[columnName] = func() any {
			v := int64(0)
			return &v
		}
		e.mapPointerToValue[columnName] = func(val any) any {
			return *val.(*int64)
		}
		e.fieldBindSetters[columnName] = createNumberFieldBindSetter(columnName, false, false, min, max)
		e.columnAttrToStringSetters[columnName] = createInt64AttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createNumberFieldSetter(attributes, false, false)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildIntPointerField(attributes schemaFieldAttributes, min int64, max uint64) {
	if attributes.IsArray {
		attributes.Fields.integersNullableArray = append(attributes.Fields.integersNullableArray, attributes.Index)
	} else {
		attributes.Fields.integersNullable = append(attributes.Fields.integersNullable, attributes.Index)
	}
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			if attributes.IsArray {
				switch attributes.TypeName {
				case "*int":
					attributes.Fields.integersNullableSizeArray = append(attributes.Fields.integersNullableSizeArray, 0)
				case "*int8":
					attributes.Fields.integersNullableSizeArray = append(attributes.Fields.integersNullableSizeArray, 8)
				case "*int16":
					attributes.Fields.integersNullableSizeArray = append(attributes.Fields.integersNullableSizeArray, 16)
				case "*int32":
					attributes.Fields.integersNullableSizeArray = append(attributes.Fields.integersNullableSizeArray, 32)
				case "*int64":
					attributes.Fields.integersNullableSizeArray = append(attributes.Fields.integersNullableSizeArray, 64)
				}
			} else {
				switch attributes.TypeName {
				case "*int":
					attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 0)
				case "*int8":
					attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 8)
				case "*int16":
					attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 16)
				case "*int32":
					attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 32)
				case "*int64":
					attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 64)
				}
			}
		}
		e.mapBindToScanPointer[columnName] = scanIntNullablePointer
		e.mapPointerToValue[columnName] = pointerIntNullableScan
		e.fieldBindSetters[columnName] = createNumberFieldBindSetter(columnName, false, true, min, max)
		e.columnAttrToStringSetters[columnName] = createInt64AttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createNumberFieldSetter(attributes, false, true)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildEnumField(attributes schemaFieldAttributes, enumName string, definition any) {
	if attributes.IsArray {
		attributes.Fields.stringsEnumsArray = append(attributes.Fields.stringsEnumsArray, attributes.Index)
	} else {
		attributes.Fields.stringsEnums = append(attributes.Fields.stringsEnums, attributes.Index)
	}
	for i, columnName := range attributes.GetColumnNames() {
		def := initEnumDefinition(enumName, definition, attributes.Tags["required"] == "true")
		if i == 0 {
			if attributes.IsArray {
				attributes.Fields.enumsArray = append(attributes.Fields.enumsArray, def)
			} else {
				attributes.Fields.enums = append(attributes.Fields.enums, def)
			}
		}

		e.mapBindToScanPointer[columnName] = func() any {
			return &sql.NullString{}
		}
		e.mapPointerToValue[columnName] = func(val any) any {
			v := val.(*sql.NullString)
			if v.Valid {
				return v.String
			}
			return nil
		}
		stringSetter := createStringFieldBindSetter(columnName, 0, def.required)
		e.fieldBindSetters[columnName] = createEnumFieldBindSetter(columnName, stringSetter, def)
		e.columnAttrToStringSetters[columnName] = createStringAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createStringFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildStringField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.stringsArray = append(attributes.Fields.stringsArray, attributes.Index)
	} else {
		attributes.Fields.strings = append(attributes.Fields.strings, attributes.Index)
	}
	for i, columnName := range attributes.GetColumnNames() {
		isRequired := false
		stringLength := 255
		if i == 0 {
			isRequired = attributes.Tags["required"] == "true"
			length := attributes.Tags["length"]
			if length == "max" {
				stringLength = 16777215
			} else if length != "" {
				stringLength, _ = strconv.Atoi(length)
			}
			if attributes.IsArray {
				attributes.Fields.stringMaxLengthsArray = append(attributes.Fields.stringMaxLengthsArray, stringLength)
				attributes.Fields.stringsRequiredArray = append(attributes.Fields.stringsRequiredArray, isRequired)
			} else {
				attributes.Fields.stringMaxLengths = append(attributes.Fields.stringMaxLengths, stringLength)
				attributes.Fields.stringsRequired = append(attributes.Fields.stringsRequired, isRequired)
			}
		}
		e.mapBindToScanPointer[columnName] = func() any {
			return &sql.NullString{}
		}
		e.mapPointerToValue[columnName] = func(val any) any {
			v := val.(*sql.NullString)
			if v.Valid {
				return v.String
			}
			return nil
		}
		e.fieldBindSetters[columnName] = createStringFieldBindSetter(columnName, stringLength, isRequired)
		e.columnAttrToStringSetters[columnName] = createStringAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createStringFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildBytesField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.bytesArray = append(attributes.Fields.bytesArray, attributes.Index)
	} else {
		attributes.Fields.bytes = append(attributes.Fields.bytes, attributes.Index)
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.columnAttrToStringSetters[columnName] = createNotSupportedAttrToStringSetter(columnName)
		e.fieldBindSetters[columnName] = createBytesFieldBindSetter(columnName)
		e.fieldSetters[columnName] = createBytesFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildStringSliceField(enumName string, attributes schemaFieldAttributes, definition any) {
	if attributes.IsArray {
		attributes.Fields.sliceStringsSetsArray = append(attributes.Fields.sliceStringsSetsArray, attributes.Index)
	} else {
		attributes.Fields.sliceStringsSets = append(attributes.Fields.sliceStringsSets, attributes.Index)
	}
	for i, columnName := range attributes.GetColumnNames() {
		def := initEnumDefinition(enumName, definition, attributes.Tags["required"] == "true")
		if i == 0 {
			if attributes.IsArray {
				attributes.Fields.setsArray = append(attributes.Fields.setsArray, def)
			} else {
				attributes.Fields.sets = append(attributes.Fields.sets, def)
			}
		}
		e.mapBindToScanPointer[columnName] = scanStringNullablePointer
		e.mapPointerToValue[columnName] = pointerStringNullableScan
		e.columnAttrToStringSetters[columnName] = createNotSupportedAttrToStringSetter(columnName)
		stringSetter := createStringFieldBindSetter(columnName, 0, def.required)
		enumSetter := createEnumFieldBindSetter(columnName, stringSetter, def)
		e.fieldBindSetters[columnName] = createSetFieldBindSetter(columnName, enumSetter, def)
		e.fieldSetters[columnName] = createSetFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildBoolField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.booleansArray = append(attributes.Fields.booleansArray, attributes.Index)
	} else {
		attributes.Fields.booleans = append(attributes.Fields.booleans, attributes.Index)
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.mapBindToScanPointer[columnName] = scanBoolPointer
		e.mapPointerToValue[columnName] = pointerBoolScan
		e.fieldBindSetters[columnName] = createBoolFieldBindSetter(columnName)
		e.columnAttrToStringSetters[columnName] = createBoolAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createBoolFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildBoolPointerField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.booleansNullableArray = append(attributes.Fields.booleansNullableArray, attributes.Index)
	} else {
		attributes.Fields.booleansNullable = append(attributes.Fields.booleansNullable, attributes.Index)
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.mapBindToScanPointer[columnName] = scanBoolNullablePointer
		e.mapPointerToValue[columnName] = pointerBoolNullableScan
		boolSetter := createBoolFieldBindSetter(columnName)
		e.fieldBindSetters[columnName] = createNullableFieldBindSetter(boolSetter)
		e.columnAttrToStringSetters[columnName] = createBoolAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createBoolNullableFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildFloatField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.floatsArray = append(attributes.Fields.floatsArray, attributes.Index)
	} else {
		attributes.Fields.floats = append(attributes.Fields.floats, attributes.Index)
	}
	precision := 8
	decimalSize := -1
	unsigned := false
	floatBitSize := 32
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			if attributes.TypeName == "float64" {
				floatBitSize = 64
			}
			if floatBitSize == 32 {
				precision = 4
				if attributes.IsArray {
					attributes.Fields.floatsSizeArray = append(attributes.Fields.floatsSizeArray, 64)
				} else {
					attributes.Fields.floatsSize = append(attributes.Fields.floatsSize, 64)
				}
			} else {
				if attributes.IsArray {
					attributes.Fields.floatsSizeArray = append(attributes.Fields.floatsSizeArray, 64)
				} else {
					attributes.Fields.floatsSize = append(attributes.Fields.floatsSize, 64)
				}
			}
			precisionAttribute, has := attributes.Tags["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			} else {
				decimal, isDecimal := attributes.Tags["decimal"]
				if isDecimal {
					decimalArgs := strings.Split(decimal, ",")
					precision, _ = strconv.Atoi(decimalArgs[1])
					decimalSize, _ = strconv.Atoi(decimalArgs[0])
					decimalSize -= precision
				}
			}
			unsigned = attributes.Tags["unsigned"] == "true"
			if attributes.IsArray {
				attributes.Fields.floatsPrecisionArray = append(attributes.Fields.floatsPrecisionArray, precision)
				attributes.Fields.floatsDecimalSizeArray = append(attributes.Fields.floatsDecimalSizeArray, decimalSize)
				attributes.Fields.floatsUnsignedArray = append(attributes.Fields.floatsUnsignedArray, unsigned)
			} else {
				attributes.Fields.floatsPrecision = append(attributes.Fields.floatsPrecision, precision)
				attributes.Fields.floatsDecimalSize = append(attributes.Fields.floatsDecimalSize, decimalSize)
				attributes.Fields.floatsUnsigned = append(attributes.Fields.floatsUnsigned, unsigned)
			}
		}
		e.mapBindToScanPointer[columnName] = func() any {
			v := float64(0)
			return &v
		}
		e.mapPointerToValue[columnName] = func(val any) any {
			return *val.(*float64)
		}
		e.fieldBindSetters[columnName] = createFloatFieldBindSetter(columnName, unsigned, false, precision, floatBitSize, decimalSize)
		e.columnAttrToStringSetters[columnName] = createFloatAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createFloatFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildFloatPointerField(attributes schemaFieldAttributes) {
	if attributes.IsArray {
		attributes.Fields.floatsNullableArray = append(attributes.Fields.floatsNullableArray, attributes.Index)
	} else {
		attributes.Fields.floatsNullable = append(attributes.Fields.floatsNullable, attributes.Index)
	}
	unsigned := false
	precision := 8
	decimalSize := -1
	floatBitSize := 32
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			unsigned = attributes.Tags["unsigned"] == "true"
			if attributes.TypeName == "*float64" {
				floatBitSize = 64
			}
			if floatBitSize == 32 {
				precision = 4
				if attributes.IsArray {
					attributes.Fields.floatsNullableSizeArray = append(attributes.Fields.floatsNullableSizeArray, 32)
				} else {
					attributes.Fields.floatsNullableSize = append(attributes.Fields.floatsNullableSize, 32)
				}
			} else {
				if attributes.IsArray {
					attributes.Fields.floatsNullableSizeArray = append(attributes.Fields.floatsNullableSizeArray, 64)
				} else {
					attributes.Fields.floatsNullableSize = append(attributes.Fields.floatsNullableSize, 64)
				}
			}
			precisionAttribute, has := attributes.Tags["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			} else {
				decimal, isDecimal := attributes.Tags["decimal"]
				if isDecimal {
					decimalArgs := strings.Split(decimal, ",")
					precision, _ = strconv.Atoi(decimalArgs[1])
					decimalSize, _ = strconv.Atoi(decimalArgs[0])
					decimalSize -= precision
				}
			}
			if attributes.IsArray {
				attributes.Fields.floatsNullablePrecisionArray = append(attributes.Fields.floatsNullablePrecisionArray, precision)
				attributes.Fields.floatsNullableDecimalSizeArray = append(attributes.Fields.floatsNullableDecimalSizeArray, decimalSize)
				attributes.Fields.floatsNullableUnsignedArray = append(attributes.Fields.floatsNullableUnsignedArray, unsigned)
			} else {
				attributes.Fields.floatsNullablePrecision = append(attributes.Fields.floatsNullablePrecision, precision)
				attributes.Fields.floatsNullableDecimalSize = append(attributes.Fields.floatsNullableDecimalSize, decimalSize)
				attributes.Fields.floatsNullableUnsigned = append(attributes.Fields.floatsNullableUnsigned, unsigned)
			}
		}
		e.mapBindToScanPointer[columnName] = scanFloatNullablePointer
		e.mapPointerToValue[columnName] = pointerFloatNullableScan
		e.columnAttrToStringSetters[columnName] = createNotSupportedAttrToStringSetter(columnName)
		floatSetter := createFloatFieldBindSetter(columnName, unsigned, false, precision, floatBitSize, decimalSize)
		e.fieldBindSetters[columnName] = createNullableFieldBindSetter(floatSetter)
		e.fieldSetters[columnName] = createFloatNullableFieldSetter(attributes)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildTimePointerField(attributes schemaFieldAttributes) {
	_, hasTime := attributes.Tags["time"]
	if attributes.IsArray {
		if hasTime {
			attributes.Fields.timesNullableArray = append(attributes.Fields.timesNullableArray, attributes.Index)
		} else {
			attributes.Fields.datesNullableArray = append(attributes.Fields.datesNullableArray, attributes.Index)
		}
	} else {
		if hasTime {
			attributes.Fields.timesNullable = append(attributes.Fields.timesNullable, attributes.Index)
		} else {
			attributes.Fields.datesNullable = append(attributes.Fields.datesNullable, attributes.Index)
		}
	}
	layout := time.DateOnly
	if hasTime {
		layout = time.DateTime
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.mapBindToScanPointer[columnName] = scanStringNullablePointer
		e.mapPointerToValue[columnName] = pointerStringNullableScan
		timeSetter := createDateFieldBindSetter(columnName, layout, true)
		e.fieldBindSetters[columnName] = createNullableFieldBindSetter(timeSetter)
		e.columnAttrToStringSetters[columnName] = createDateTimeAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createTimeNullableFieldSetter(attributes, layout)
		e.fieldGetters[columnName] = createFieldGetter(attributes, true)
	}
}

func (e *entitySchema) buildTimeField(attributes schemaFieldAttributes) {
	_, hasTime := attributes.Tags["time"]
	if attributes.IsArray {
		if hasTime {
			attributes.Fields.timesArray = append(attributes.Fields.timesArray, attributes.Index)
		} else {
			attributes.Fields.datesArray = append(attributes.Fields.datesArray, attributes.Index)
		}
	} else {
		if hasTime {
			attributes.Fields.times = append(attributes.Fields.times, attributes.Index)
		} else {
			attributes.Fields.dates = append(attributes.Fields.dates, attributes.Index)
		}
	}
	layout := time.DateOnly
	if hasTime {
		layout = time.DateTime
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.mapBindToScanPointer[columnName] = scanStringPointer
		e.mapPointerToValue[columnName] = pointerStringScan
		e.fieldBindSetters[columnName] = createDateFieldBindSetter(columnName, layout, false)
		e.columnAttrToStringSetters[columnName] = createDateTimeAttrToStringSetter(e.fieldBindSetters[columnName])
		e.fieldSetters[columnName] = createTimeFieldSetter(attributes, layout)
		e.fieldGetters[columnName] = createFieldGetter(attributes, false)
	}
}

func (e *entitySchema) buildStructField(attributes schemaFieldAttributes, registry *registry,
	schemaTags map[string]map[string]string) {
	var parents []int
	if attributes.Parents != nil {
		parents = append(parents, attributes.Parents...)
	}
	parents = append(parents, attributes.Index)
	if attributes.IsArray {
		attributes.Fields.structsArray = append(attributes.Fields.structsArray, attributes.Index)
		subFields := e.buildTableFields(attributes.Field.Type.Elem(), registry, 0, attributes.Field.Name, parents, schemaTags)
		attributes.Fields.structsFieldsArray = append(attributes.Fields.structsFieldsArray, subFields)
	} else {
		attributes.Fields.structs = append(attributes.Fields.structs, attributes.Index)
		subPrefix := ""
		if !attributes.Field.Anonymous {
			subPrefix = attributes.Field.Name
		}
		subFields := e.buildTableFields(attributes.Field.Type, registry, 0, subPrefix, parents, schemaTags)
		attributes.Fields.structsFields = append(attributes.Fields.structsFields, subFields)
	}
}

func extractTags(registry *registry, entityType reflect.Type, prefix string) (fields map[string]map[string]string) {
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
		name := prefix + field.Name
		refOne := ""
		hasRef := false
		if field.Type.Kind().String() == "ptr" {
			refName := field.Type.Elem().String()
			_, hasRef = registry.entities[refName]
			if hasRef {
				refOne = refName
			}
		}

		query, hasQuery := field.Tag.Lookup("query")
		queryOne, hasQueryOne := field.Tag.Lookup("queryOne")
		if hasQuery {
			if fields[name] == nil {
				fields[name] = make(map[string]string)
			}
			fields[name]["query"] = query
		}
		if hasQueryOne {
			if fields[name] == nil {
				fields[name] = make(map[string]string)
			}
			fields[field.Name]["queryOne"] = queryOne
		}
		if hasRef {
			if fields[name] == nil {
				fields[name] = make(map[string]string)
			}
			fields[name]["ref"] = refOne
			fields[name]["refPath"] = field.Name
			if prefix != "" {
				fields[name]["refPath"] = prefix + "." + field.Name
			}
		}
	}
	return
}

func extractTag(registry *registry, field reflect.StructField) map[string]map[string]string {
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
		if t != "time.Time" {
			prefix := ""
			if !field.Anonymous {
				prefix = field.Name
			}
			return extractTags(registry, field.Type, prefix)
		}
	}
	return make(map[string]map[string]string)
}

func (fields *tableFields) buildColumnNames(subFieldPrefix string) ([]string, string) {
	fieldsQuery := ""
	columns := make([]string, 0)
	ids := fields.uIntegers
	ids = append(ids, fields.uIntegersArray...)
	ids = append(ids, fields.references...)
	ids = append(ids, fields.referencesArray...)
	ids = append(ids, fields.integers...)
	ids = append(ids, fields.integersArray...)
	ids = append(ids, fields.booleans...)
	ids = append(ids, fields.booleansArray...)
	ids = append(ids, fields.floats...)
	ids = append(ids, fields.floatsArray...)
	ids = append(ids, fields.times...)
	ids = append(ids, fields.timesArray...)
	ids = append(ids, fields.dates...)
	ids = append(ids, fields.datesArray...)
	ids = append(ids, fields.strings...)
	ids = append(ids, fields.stringsArray...)
	ids = append(ids, fields.uIntegersNullable...)
	ids = append(ids, fields.uIntegersNullableArray...)
	ids = append(ids, fields.integersNullable...)
	ids = append(ids, fields.integersNullableArray...)
	ids = append(ids, fields.stringsEnums...)
	ids = append(ids, fields.stringsEnumsArray...)
	ids = append(ids, fields.bytes...)
	ids = append(ids, fields.bytesArray...)
	ids = append(ids, fields.sliceStringsSets...)
	ids = append(ids, fields.sliceStringsSetsArray...)
	ids = append(ids, fields.booleansNullable...)
	ids = append(ids, fields.booleansNullableArray...)
	ids = append(ids, fields.floatsNullable...)
	ids = append(ids, fields.floatsNullableArray...)
	ids = append(ids, fields.timesNullable...)
	ids = append(ids, fields.timesNullableArray...)
	ids = append(ids, fields.datesNullable...)
	ids = append(ids, fields.datesNullableArray...)
	for _, index := range ids {
		l := fields.arrays[index]
		if l > 0 {
			for i := 1; i <= l; i++ {
				name := subFieldPrefix + fields.fields[index].Name + "_" + strconv.Itoa(i)
				columns = append(columns, name)
				fieldsQuery += ",`" + name + "`"
			}
		} else {
			name := subFieldPrefix + fields.fields[index].Name
			columns = append(columns, name)
			fieldsQuery += ",`" + name + "`"
		}
	}
	for i, subFields := range fields.structsFields {
		field := fields.fields[fields.structs[i]]
		prefixName := subFieldPrefix
		if !field.Anonymous {
			prefixName += field.Name
		}
		subColumns, subQuery := subFields.buildColumnNames(prefixName)
		columns = append(columns, subColumns...)
		fieldsQuery += subQuery
	}
	for z, k := range fields.structsArray {
		l := fields.arrays[k]
		for i := 1; i <= l; i++ {
			attr := fields.structsFieldsArray[z]
			subColumns, subQuery := attr.buildColumnNames(attr.prefix + "_" + strconv.Itoa(i) + "_")
			columns = append(columns, subColumns...)
			fieldsQuery += subQuery
		}
	}
	return columns, fieldsQuery
}

var scanIntNullablePointer = func() any {
	return &sql.NullInt64{}
}

var pointerUintNullableScan = func(val any) any {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return uint64(v.Int64)
	}
	return nil
}

var pointerIntNullableScan = func(val any) any {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return v.Int64
	}
	return nil
}

var scanStringNullablePointer = func() any {
	return &sql.NullString{}
}

var pointerStringNullableScan = func(val any) any {
	v := val.(*sql.NullString)
	if v.Valid {
		return v.String
	}
	return nil
}

var scanBoolPointer = func() any {
	v := false
	return &v
}

var pointerBoolScan = func(val any) any {
	return *val.(*bool)
}

var scanBoolNullablePointer = func() any {
	return &sql.NullBool{}
}

var pointerBoolNullableScan = func(val any) any {
	v := val.(*sql.NullBool)
	if v.Valid {
		return v.Bool
	}
	return nil
}

var scanFloatNullablePointer = func() any {
	return &sql.NullFloat64{}
}

var pointerFloatNullableScan = func(val any) any {
	v := val.(*sql.NullFloat64)
	if v.Valid {
		return v.Float64
	}
	return nil
}

var scanStringPointer = func() any {
	v := ""
	return &v
}

var pointerStringScan = func(val any) any {
	return *val.(*string)
}
