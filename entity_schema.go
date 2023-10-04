package beeorm

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const flushLazyEventsList = "flush_lazy_events"
const flushLazyEventsListErrorSuffix = ":err"

var codeStartTime = uint64(time.Now().Unix())

func GetEntitySchema[E any](c Context) EntitySchema {
	return getEntitySchema[E](c)
}

func getEntitySchema[E any](c Context) *entitySchema {
	ci := c.(*contextImplementation)
	var entity E
	schema, has := ci.engine.registry.entitySchemas[reflect.TypeOf(entity)]
	if !has {
		panic(fmt.Errorf("entity '%T' is not registered", entity))
	}
	return schema
}

type EntitySchema interface {
	GetTableName() string
	GetEntityName() string
	GetType() reflect.Type
	DropTable(c Context)
	TruncateTable(c Context)
	UpdateSchema(c Context)
	UpdateSchemaAndTruncateTable(c Context)
	GetDB() DB
	GetLocalCache() (cache LocalCache, has bool)
	GetRedisCache() (cache RedisCache, has bool)
	GetColumns() []string
	GetUniqueIndexes() map[string][]string
	GetSchemaChanges(c Context) (has bool, alters []Alter)
	GetTag(field, key, trueValue, defaultValue string) string
	GetPluginOption(plugin, key string) interface{}
	GetCacheKey() string
	DisableCache(local, redis bool)
	getFields() *tableFields
	getTagBool(field, key string) bool
	getFieldsQuery() string
	getStructureHash() string
	getTags() map[string]map[string]string
	uuid() uint64
	getLazyRedisCode() string
}

type SettableEntitySchema interface {
	EntitySchema
	SetPluginOption(plugin, key string, value interface{})
}

type columnAttrToStringSetter func(v any) (string, error)

type entitySchema struct {
	tableName                 string
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
	uniqueIndices             map[string][]string
	hasLocalCache             bool
	localCache                *localCache
	localCacheLimit           int
	redisCacheName            string
	hasRedisCache             bool
	redisCache                *redisCache
	searchCacheName           string
	cacheKey                  string
	lazyCacheKey              string
	structureHash             string
	skipLogs                  []string
	mapBindToScanPointer      mapBindToScanPointer
	mapPointerToValue         mapPointerToValue
	options                   map[string]map[string]interface{}
	uuidServerID              uint64
	uuidCounter               uint64
}

type mapBindToScanPointer map[string]func() interface{}
type mapPointerToValue map[string]func(val interface{}) interface{}

type tableFields struct {
	t                         reflect.Type
	fields                    map[int]reflect.StructField
	forcedOldBid              map[int]bool
	prefix                    string
	uIntegers                 []int
	integers                  []int
	references                []int
	referencesRequired        []bool
	uIntegersNullable         []int
	uIntegersNullableSize     []int
	integersNullable          []int
	integersNullableSize      []int
	strings                   []int
	stringMaxLengths          []int
	stringsRequired           []bool
	stringsEnums              []int
	enums                     []*enumDefinition
	sliceStringsSets          []int
	sets                      []*enumDefinition
	bytes                     []int
	booleans                  []int
	booleansNullable          []int
	floats                    []int
	floatsPrecision           []int
	floatsDecimalSize         []int
	floatsSize                []int
	floatsUnsigned            []bool
	floatsNullable            []int
	floatsNullablePrecision   []int
	floatsNullableDecimalSize []int
	floatsNullableUnsigned    []bool
	floatsNullableSize        []int
	timesNullable             []int
	datesNullable             []int
	times                     []int
	dates                     []int
	structs                   []int
	structsFields             []*tableFields
}

func (entitySchema *entitySchema) GetTableName() string {
	return entitySchema.tableName
}

func (entitySchema *entitySchema) GetEntityName() string {
	return entitySchema.t.String()
}

func (entitySchema *entitySchema) GetType() reflect.Type {
	return entitySchema.t
}

func (entitySchema *entitySchema) DropTable(c Context) {
	pool := entitySchema.GetDB()
	pool.Exec(c, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetPoolConfig().GetDatabase(), entitySchema.tableName))
}

func (entitySchema *entitySchema) TruncateTable(c Context) {
	pool := entitySchema.GetDB()
	_ = pool.Exec(c, fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetPoolConfig().GetDatabase(), entitySchema.tableName))
	_ = pool.Exec(c, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetPoolConfig().GetDatabase(), entitySchema.tableName))
}

func (entitySchema *entitySchema) UpdateSchema(c Context) {
	pool := entitySchema.GetDB()
	has, alters := entitySchema.GetSchemaChanges(c)
	if has {
		for _, alter := range alters {
			_ = pool.Exec(c, alter.SQL)
		}
	}
}

func (entitySchema *entitySchema) UpdateSchemaAndTruncateTable(c Context) {
	entitySchema.UpdateSchema(c)
	pool := entitySchema.GetDB()
	_ = pool.Exec(c, fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetPoolConfig().GetDatabase(), entitySchema.tableName))
	_ = pool.Exec(c, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetPoolConfig().GetDatabase(), entitySchema.tableName))
}

func (entitySchema *entitySchema) GetDB() DB {
	return entitySchema.engine.DB(entitySchema.mysqlPoolCode)
}

func (entitySchema *entitySchema) GetLocalCache() (cache LocalCache, has bool) {
	if !entitySchema.hasLocalCache {
		return nil, false
	}
	return entitySchema.localCache, true
}

func (entitySchema *entitySchema) GetRedisCache() (cache RedisCache, has bool) {
	if !entitySchema.hasRedisCache {
		return nil, false
	}
	return entitySchema.redisCache, true
}

func (entitySchema *entitySchema) GetColumns() []string {
	return entitySchema.columnNames
}

func (entitySchema *entitySchema) GetUniqueIndexes() map[string][]string {
	return entitySchema.uniqueIndices
}

func (entitySchema *entitySchema) GetSchemaChanges(c Context) (has bool, alters []Alter) {
	pre, alters, post := getSchemaChanges(c, entitySchema)
	final := pre
	final = append(final, alters...)
	final = append(final, post...)
	return len(final) > 0, final
}

func (entitySchema *entitySchema) init(registry *Registry, entityType reflect.Type) error {
	entitySchema.t = entityType
	entitySchema.tSlice = reflect.SliceOf(reflect.PtrTo(entityType))
	entitySchema.tags = extractTags(registry, entityType, "")
	entitySchema.mapBindToScanPointer = mapBindToScanPointer{}
	entitySchema.mapPointerToValue = mapPointerToValue{}
	entitySchema.mysqlPoolCode = entitySchema.getTag("mysql", "default", DefaultPoolCode)
	_, has := registry.mysqlPools[entitySchema.mysqlPoolCode]
	if !has {
		return fmt.Errorf("mysql pool '%s' not found", entitySchema.mysqlPoolCode)
	}
	entitySchema.tableName = entitySchema.getTag("table", entityType.Name(), entityType.Name())
	localCacheLimit := entitySchema.getTag("localCache", DefaultPoolCode, "")
	redisCacheName := entitySchema.getTag("redisCache", DefaultPoolCode, "")
	if redisCacheName != "" {
		_, has = registry.redisPools[redisCacheName]
		if !has {
			return fmt.Errorf("redis pool '%s' not found", redisCacheName)
		}
	}
	cacheKey := ""
	if entitySchema.mysqlPoolCode != DefaultPoolCode {
		cacheKey = entitySchema.mysqlPoolCode
	}
	cacheKey += entitySchema.tableName
	uniqueIndices := make(map[string]map[int]string)
	uniqueIndicesSimple := make(map[string][]string)
	uniqueIndicesSimpleGlobal := make(map[string][]string)
	indices := make(map[string]map[int]string)
	uniqueGlobal := entitySchema.getTag("unique", "", "")
	if uniqueGlobal != "" {
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
	for k, v := range entitySchema.tags {
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
	}
	entitySchema.columnAttrToStringSetters = make(map[string]columnAttrToStringSetter)
	entitySchema.fields = entitySchema.buildTableFields(entityType, registry, 0, "", entitySchema.tags)
	entitySchema.columnNames, entitySchema.fieldsQuery = entitySchema.fields.buildColumnNames("")
	if len(entitySchema.fieldsQuery) > 0 {
		entitySchema.fieldsQuery = entitySchema.fieldsQuery[1:]
	}
	columnMapping := make(map[string]int)
	for i, name := range entitySchema.columnNames {
		columnMapping[name] = i
	}
	cacheKey = hashString(cacheKey + entitySchema.fieldsQuery)
	cacheKey = cacheKey[0:5]
	h := fnv.New32a()
	_, _ = h.Write([]byte(cacheKey))

	entitySchema.structureHash = strconv.FormatUint(uint64(h.Sum32()), 10)
	entitySchema.columnMapping = columnMapping
	entitySchema.hasLocalCache = localCacheLimit != ""
	if entitySchema.hasLocalCache {
		limit := 100000
		if localCacheLimit != DefaultPoolCode {
			userLimit, err := strconv.Atoi(localCacheLimit)
			if err != nil || userLimit <= 0 {
				return fmt.Errorf("invalid local cache limit for '%s'", entitySchema.t.String())
			}
			limit = userLimit
		}
		entitySchema.localCacheLimit = limit
	}
	entitySchema.redisCacheName = redisCacheName
	entitySchema.hasRedisCache = redisCacheName != ""
	entitySchema.cacheKey = cacheKey

	lazyList := entitySchema.getTag("custom_lazy_group", entitySchema.t.String(), "")
	if lazyList == "" {
		lazyList = flushLazyEventsList
	}
	entitySchema.lazyCacheKey = entitySchema.mysqlPoolCode + ":" + lazyList

	entitySchema.uniqueIndices = uniqueIndicesSimple
	for _, plugin := range registry.plugins {
		interfaceInitEntitySchema, isInterfaceInitEntitySchema := plugin.(PluginInterfaceInitEntitySchema)
		if isInterfaceInitEntitySchema {
			err := interfaceInitEntitySchema.InterfaceInitEntitySchema(entitySchema, registry)
			if err != nil {
				return err
			}
		}
	}
	return entitySchema.validateIndexes(uniqueIndices, indices)
}

func (entitySchema *entitySchema) validateIndexes(uniqueIndices map[string]map[int]string, indices map[string]map[int]string) error {
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
				return fmt.Errorf("duplicated index %s with %s in %s", k, k2, entitySchema.t.String())
			}
		}
	}
	return nil
}

func (entitySchema *entitySchema) getTag(key, trueValue, defaultValue string) string {
	userValue, has := entitySchema.tags["ID"][key]
	if has {
		if userValue == "true" {
			return trueValue
		}
		return userValue
	}
	return entitySchema.GetTag("ID", key, trueValue, defaultValue)
}

func (entitySchema *entitySchema) GetTag(field, key, trueValue, defaultValue string) string {
	userValue, has := entitySchema.tags[field][key]
	if has {
		if userValue == "true" {
			return trueValue
		}
		return userValue
	}
	return defaultValue
}

func (entitySchema *entitySchema) getTagBool(field, key string) bool {
	tag := entitySchema.GetTag(field, key, "1", "")
	return tag == "1"
}

func (entitySchema *entitySchema) getFieldsQuery() string {
	return entitySchema.fieldsQuery
}

func (entitySchema *entitySchema) getStructureHash() string {
	return entitySchema.structureHash
}

func (entitySchema *entitySchema) getTags() map[string]map[string]string {
	return entitySchema.tags
}

func (entitySchema *entitySchema) uuid() uint64 {
	return (entitySchema.uuidServerID&255)<<56 + (codeStartTime << 24) + atomic.AddUint64(&entitySchema.uuidCounter, 1)
}

func (entitySchema *entitySchema) getLazyRedisCode() string {
	if entitySchema.hasRedisCache {
		return entitySchema.redisCacheName
	}
	return DefaultPoolCode
}

func (entitySchema *entitySchema) GetPluginOption(plugin, key string) interface{} {
	if entitySchema.options == nil {
		return nil
	}
	values, has := entitySchema.options[plugin]
	if !has {
		return nil
	}
	return values[key]
}

func (entitySchema *entitySchema) GetCacheKey() string {
	return entitySchema.cacheKey
}

func (entitySchema *entitySchema) DisableCache(local, redis bool) {
	if local {
		entitySchema.localCacheLimit = 0
		entitySchema.hasLocalCache = false
	}
	if redis {
		entitySchema.redisCacheName = ""
		entitySchema.hasRedisCache = false
	}
}

func (entitySchema *entitySchema) SetPluginOption(plugin, key string, value interface{}) {
	if entitySchema.options == nil {
		entitySchema.options = map[string]map[string]interface{}{plugin: {key: value}}
	} else {
		before, has := entitySchema.options[plugin]
		if !has {
			entitySchema.options[plugin] = map[string]interface{}{key: value}
		} else {
			before[key] = value
		}
	}
}

func (entitySchema *entitySchema) buildTableFields(t reflect.Type, registry *Registry,
	start int, prefix string, schemaTags map[string]map[string]string) *tableFields {
	fields := &tableFields{t: t, prefix: prefix, fields: make(map[int]reflect.StructField)}
	fields.forcedOldBid = make(map[int]bool)
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
			Prefix:   prefix,
			Field:    f,
			TypeName: f.Type.String(),
		}
		fields.fields[i] = f
		switch attributes.TypeName {
		case "uint",
			"uint8",
			"uint16",
			"uint32",
			"uint64":
			entitySchema.buildUintField(attributes)
		case "*uint",
			"*uint8",
			"*uint16",
			"*uint32",
			"*uint64":
			entitySchema.buildUintPointerField(attributes)
		case "int",
			"int8",
			"int16",
			"int32",
			"int64":
			entitySchema.buildIntField(attributes)
		case "*int",
			"*int8",
			"*int16",
			"*int32",
			"*int64":
			entitySchema.buildIntPointerField(attributes)
		case "string":
			entitySchema.buildStringField(attributes)
		case "[]uint8":
			fields.bytes = append(fields.bytes, i)
		case "bool":
			entitySchema.buildBoolField(attributes)
		case "*bool":
			entitySchema.buildBoolPointerField(attributes)
		case "float32",
			"float64":
			entitySchema.buildFloatField(attributes)
		case "*float32",
			"*float64":
			entitySchema.buildFloatPointerField(attributes)
		case "*time.Time":
			entitySchema.buildTimePointerField(attributes)
		case "time.Time":
			entitySchema.buildTimeField(attributes)
		default:
			k := f.Type.Kind().String()
			if k == "struct" {
				entitySchema.buildStructField(attributes, registry, schemaTags)
			} else if f.Type.Implements(reflect.TypeOf((*EnumValues)(nil)).Elem()) {
				definition := reflect.New(f.Type).Interface().(EnumValues).EnumValues()
				if f.Type.Kind().String() == "string" {
					entitySchema.buildEnumField(attributes, definition)
				} else {
					entitySchema.buildStringSliceField(attributes, definition)
				}
			} else if f.Type.Implements(reflect.TypeOf((*referenceInterface)(nil)).Elem()) {
				entitySchema.buildReferenceField(attributes)
			} else {
				panic(fmt.Errorf("field type %s is not supported", f.Type.String()))
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
	Prefix   string
}

func (attributes schemaFieldAttributes) GetColumnName() string {
	return attributes.Prefix + attributes.Field.Name
}

func (entitySchema *entitySchema) buildUintField(attributes schemaFieldAttributes) {
	attributes.Fields.uIntegers = append(attributes.Fields.uIntegers, attributes.Index)
	columnName := attributes.GetColumnName()
	entitySchema.mapBindToScanPointer[columnName] = func() interface{} {
		v := uint64(0)
		return &v
	}
	entitySchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*uint64)
	}
	entitySchema.columnAttrToStringSetters[columnName] = func(v any) (string, error) {
		switch v.(type) {
		case string:
			_, err := strconv.ParseUint(v.(string), 10, 64)
			if err != nil {
				return "", err
			}
			return v.(string), nil
		case uint8:
			return strconv.FormatUint(uint64(v.(uint8)), 10), nil
		case uint16:
			return strconv.FormatUint(uint64(v.(uint16)), 10), nil
		case uint:
			return strconv.FormatUint(uint64(v.(uint)), 10), nil
		case uint32:
			return strconv.FormatUint(uint64(v.(uint32)), 10), nil
		case uint64:
			return strconv.FormatUint(v.(uint64), 10), nil
		case int8:
			return strconv.FormatUint(uint64(v.(int8)), 10), nil
		case int16:
			return strconv.FormatUint(uint64(v.(int16)), 10), nil
		case int:
			return strconv.FormatUint(uint64(v.(int)), 10), nil
		case int32:
			return strconv.FormatUint(uint64(v.(int32)), 10), nil
		case int64:
			return strconv.FormatUint(uint64(v.(int64)), 10), nil
		default:
			return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
		}
	}
}

func (entitySchema *entitySchema) buildReferenceField(attributes schemaFieldAttributes) {
	attributes.Fields.references = append(attributes.Fields.references, attributes.Index)
	attributes.Fields.referencesRequired = append(attributes.Fields.referencesRequired, attributes.Tags["required"] == "true")
	columnName := attributes.GetColumnName()
	entitySchema.mapBindToScanPointer[columnName] = scanIntNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerUintNullableScan
	entitySchema.columnAttrToStringSetters[columnName] = func(v any) (string, error) {
		switch v.(type) {
		case uint64:
			return strconv.FormatUint(v.(uint64), 10), nil
		case int:
			return strconv.FormatUint(uint64(v.(int)), 10), nil
		default:
			asRef, valid := v.(referenceInterface)
			if valid {
				return strconv.FormatUint(asRef.GetID(), 10), nil
			}
		}
		return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
	}
}

func (entitySchema *entitySchema) buildUintPointerField(attributes schemaFieldAttributes) {
	attributes.Fields.uIntegersNullable = append(attributes.Fields.uIntegersNullable, attributes.Index)
	columnName := attributes.GetColumnName()
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
	entitySchema.mapBindToScanPointer[columnName] = scanIntNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerUintNullableScan
}

func (entitySchema *entitySchema) buildIntField(attributes schemaFieldAttributes) {
	attributes.Fields.integers = append(attributes.Fields.integers, attributes.Index)
	columnName := attributes.GetColumnName()
	entitySchema.mapBindToScanPointer[columnName] = func() interface{} {
		v := int64(0)
		return &v
	}
	entitySchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*int64)
	}
}

func (entitySchema *entitySchema) buildIntPointerField(attributes schemaFieldAttributes) {
	attributes.Fields.integersNullable = append(attributes.Fields.integersNullable, attributes.Index)
	columnName := attributes.GetColumnName()
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
	entitySchema.mapBindToScanPointer[columnName] = scanIntNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerIntNullableScan
}

func (entitySchema *entitySchema) buildEnumField(attributes schemaFieldAttributes, definition interface{}) {
	columnName := attributes.GetColumnName()
	attributes.Fields.stringsEnums = append(attributes.Fields.stringsEnums, attributes.Index)
	def := initEnumDefinition(definition, attributes.Tags["required"] == "true")
	attributes.Fields.enums = append(attributes.Fields.enums, def)
	entitySchema.mapBindToScanPointer[columnName] = func() interface{} {
		return &sql.NullString{}
	}
	entitySchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		v := val.(*sql.NullString)
		if v.Valid {
			return v.String
		}
		return nil
	}
}

func (entitySchema *entitySchema) buildStringField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	attributes.Fields.strings = append(attributes.Fields.strings, attributes.Index)
	stringLength := 255
	length := attributes.Tags["length"]
	if length == "max" {
		stringLength = 16777215
	} else if length != "" {
		stringLength, _ = strconv.Atoi(length)
	}
	attributes.Fields.stringMaxLengths = append(attributes.Fields.stringMaxLengths, stringLength)
	attributes.Fields.stringsRequired = append(attributes.Fields.stringsRequired, attributes.Tags["required"] == "true")
	entitySchema.mapBindToScanPointer[columnName] = func() interface{} {
		return &sql.NullString{}
	}
	entitySchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		v := val.(*sql.NullString)
		if v.Valid {
			return v.String
		}
		return nil
	}
	entitySchema.columnAttrToStringSetters[columnName] = func(v any) (string, error) {
		switch v.(type) {
		case string:
			return v.(string), nil
		default:
			return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
		}
	}
}

func (entitySchema *entitySchema) buildStringSliceField(attributes schemaFieldAttributes, definition interface{}) {
	columnName := attributes.GetColumnName()
	attributes.Fields.sliceStringsSets = append(attributes.Fields.sliceStringsSets, attributes.Index)
	attributes.Fields.sets = append(attributes.Fields.sets, initEnumDefinition(definition, attributes.Tags["required"] == "true"))
	entitySchema.mapBindToScanPointer[columnName] = scanStringNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerStringNullableScan
}

func (entitySchema *entitySchema) buildBoolField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	attributes.Fields.booleans = append(attributes.Fields.booleans, attributes.Index)
	entitySchema.mapBindToScanPointer[columnName] = scanBoolPointer
	entitySchema.mapPointerToValue[columnName] = pointerBoolScan
	entitySchema.columnAttrToStringSetters[columnName] = func(v any) (string, error) {
		switch v.(type) {
		case bool:
			if v.(bool) {
				return "1", nil
			}
			return "0", nil
		case string:
			s := strings.ToLower(v.(string))
			if s == "1" || s == "true" {
				return "1", nil
			} else if s == "0" || s == "false" {
				return "0", nil
			}
		case int:
			asInt := v.(int)
			if asInt == 1 {
				return "1", nil
			} else if asInt == 0 {
				return "0", nil
			}
			return strconv.FormatUint(uint64(v.(int)), 10), nil
		}
		return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
	}
}

func (entitySchema *entitySchema) buildBoolPointerField(attributes schemaFieldAttributes) {
	attributes.Fields.booleansNullable = append(attributes.Fields.booleansNullable, attributes.Index)
	columnName := attributes.GetColumnName()
	entitySchema.mapBindToScanPointer[columnName] = scanBoolNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerBoolNullableScan
}

func (entitySchema *entitySchema) buildFloatField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	precision := 8
	decimalSize := -1
	if attributes.TypeName == "float32" {
		precision = 4
		attributes.Fields.floatsSize = append(attributes.Fields.floatsSize, 64)
	} else {
		attributes.Fields.floatsSize = append(attributes.Fields.floatsSize, 64)
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
	attributes.Fields.floats = append(attributes.Fields.floats, attributes.Index)
	attributes.Fields.floatsPrecision = append(attributes.Fields.floatsPrecision, precision)
	attributes.Fields.floatsDecimalSize = append(attributes.Fields.floatsDecimalSize, decimalSize)
	attributes.Fields.floatsUnsigned = append(attributes.Fields.floatsUnsigned, attributes.Tags["unsigned"] == "true")
	entitySchema.mapBindToScanPointer[columnName] = func() interface{} {
		v := float64(0)
		return &v
	}
	entitySchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*float64)
	}
}

func (entitySchema *entitySchema) buildFloatPointerField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	precision := 8
	decimalSize := -1
	if attributes.TypeName == "*float32" {
		precision = 4
		attributes.Fields.floatsNullableSize = append(attributes.Fields.floatsNullableSize, 32)
	} else {
		attributes.Fields.floatsNullableSize = append(attributes.Fields.floatsNullableSize, 64)
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
	attributes.Fields.floatsNullable = append(attributes.Fields.floatsNullable, attributes.Index)
	attributes.Fields.floatsNullablePrecision = append(attributes.Fields.floatsNullablePrecision, precision)
	attributes.Fields.floatsNullableDecimalSize = append(attributes.Fields.floatsNullableDecimalSize, decimalSize)
	attributes.Fields.floatsNullableUnsigned = append(attributes.Fields.floatsNullableUnsigned, attributes.Tags["unsigned"] == "true")
	entitySchema.mapBindToScanPointer[columnName] = scanFloatNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerFloatNullableScan
}

func (entitySchema *entitySchema) buildTimePointerField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	_, hasTime := attributes.Tags["time"]
	if hasTime {
		attributes.Fields.timesNullable = append(attributes.Fields.timesNullable, attributes.Index)
	} else {
		attributes.Fields.datesNullable = append(attributes.Fields.datesNullable, attributes.Index)
	}
	entitySchema.mapBindToScanPointer[columnName] = scanStringNullablePointer
	entitySchema.mapPointerToValue[columnName] = pointerStringNullableScan
}

func (entitySchema *entitySchema) buildTimeField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	_, hasTime := attributes.Tags["time"]
	if hasTime {
		attributes.Fields.times = append(attributes.Fields.times, attributes.Index)
	} else {
		attributes.Fields.dates = append(attributes.Fields.dates, attributes.Index)
	}
	entitySchema.mapBindToScanPointer[columnName] = scanStringPointer
	entitySchema.mapPointerToValue[columnName] = pointerStringScan
}

func (entitySchema *entitySchema) buildStructField(attributes schemaFieldAttributes, registry *Registry,
	schemaTags map[string]map[string]string) {
	attributes.Fields.structs = append(attributes.Fields.structs, attributes.Index)
	subPrefix := ""
	if !attributes.Field.Anonymous {
		subPrefix = attributes.Field.Name
	}
	subFields := entitySchema.buildTableFields(attributes.Field.Type, registry, 0, subPrefix, schemaTags)
	attributes.Fields.structsFields = append(attributes.Fields.structsFields, subFields)
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

func (entitySchema *entitySchema) getFields() *tableFields {
	return entitySchema.fields
}

func (fields *tableFields) buildColumnNames(subFieldPrefix string) ([]string, string) {
	fieldsQuery := ""
	columns := make([]string, 0)
	ids := fields.uIntegers
	ids = append(ids, fields.references...)
	ids = append(ids, fields.integers...)
	ids = append(ids, fields.booleans...)
	ids = append(ids, fields.floats...)
	timesStart := len(ids)
	ids = append(ids, fields.times...)
	ids = append(ids, fields.dates...)
	timesEnd := len(ids)
	ids = append(ids, fields.strings...)
	ids = append(ids, fields.uIntegersNullable...)
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
	for k, i := range ids {
		name := subFieldPrefix + fields.fields[i].Name
		columns = append(columns, name)
		if (k >= timesStart && k < timesEnd) || (k >= timesNullableStart && k < timesNullableEnd) {
			fieldsQuery += ",TO_SECONDS(`" + name + "`)"
		} else {
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
	return columns, fieldsQuery
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
