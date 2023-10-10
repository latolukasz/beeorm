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
	GetCacheKey() string
	DisableCache(local, redis bool)
	getFields() *tableFields
	getTagBool(field, key string) bool
	getFieldsQuery() string
	getStructureHash() string
	getTags() map[string]map[string]string
	uuid() uint64
	getForcedRedisCode() string
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
	references                map[string]referenceDefinition
	cachedReferences          map[string]referenceDefinition
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
	uIntegersArray            []int
	integers                  []int
	references                []int
	referencesArray           []int
	referencesRequired        []bool
	referencesRequiredArray   []bool
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
	structsArray              []int
	structsFields             []*tableFields
	structsFieldsArray        []*tableFields
}

func (e *entitySchema) GetTableName() string {
	return e.tableName
}

func (e *entitySchema) GetEntityName() string {
	return e.t.String()
}

func (e *entitySchema) GetType() reflect.Type {
	return e.t
}

func (e *entitySchema) DropTable(c Context) {
	pool := e.GetDB()
	pool.Exec(c, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetPoolConfig().GetDatabase(), e.tableName))
}

func (e *entitySchema) TruncateTable(c Context) {
	pool := e.GetDB()
	_ = pool.Exec(c, fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetPoolConfig().GetDatabase(), e.tableName))
	_ = pool.Exec(c, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetPoolConfig().GetDatabase(), e.tableName))
}

func (e *entitySchema) UpdateSchema(c Context) {
	pool := e.GetDB()
	has, alters := e.GetSchemaChanges(c)
	if has {
		for _, alter := range alters {
			_ = pool.Exec(c, alter.SQL)
		}
	}
}

func (e *entitySchema) UpdateSchemaAndTruncateTable(c Context) {
	e.UpdateSchema(c)
	pool := e.GetDB()
	_ = pool.Exec(c, fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetPoolConfig().GetDatabase(), e.tableName))
	_ = pool.Exec(c, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetPoolConfig().GetDatabase(), e.tableName))
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

func (e *entitySchema) GetSchemaChanges(c Context) (has bool, alters []Alter) {
	pre, alters, post := getSchemaChanges(c, e)
	final := pre
	final = append(final, alters...)
	final = append(final, post...)
	return len(final) > 0, final
}

func (e *entitySchema) init(registry *Registry, entityType reflect.Type) error {
	e.t = entityType
	e.tSlice = reflect.SliceOf(reflect.PtrTo(entityType))
	e.tags = extractTags(registry, entityType, "")
	e.references = make(map[string]referenceDefinition)
	e.cachedReferences = make(map[string]referenceDefinition)
	e.mapBindToScanPointer = mapBindToScanPointer{}
	e.mapPointerToValue = mapPointerToValue{}
	e.mysqlPoolCode = e.getTag("mysql", "default", DefaultPoolCode)
	_, has := registry.mysqlPools[e.mysqlPoolCode]
	if !has {
		return fmt.Errorf("mysql pool '%s' not found", e.mysqlPoolCode)
	}
	e.tableName = e.getTag("table", entityType.Name(), entityType.Name())
	localCacheLimit := e.getTag("localCache", DefaultPoolCode, "")
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
	e.fields = e.buildTableFields(entityType, registry, 0, "", e.tags)
	e.columnNames, e.fieldsQuery = e.fields.buildColumnNames("")
	if len(e.fieldsQuery) > 0 {
		e.fieldsQuery = e.fieldsQuery[1:]
	}
	columnMapping := make(map[string]int)
	for i, name := range e.columnNames {
		columnMapping[name] = i
	}
	cacheKey = hashString(cacheKey + e.fieldsQuery)
	cacheKey = cacheKey[0:5]
	h := fnv.New32a()
	_, _ = h.Write([]byte(cacheKey))

	e.structureHash = strconv.FormatUint(uint64(h.Sum32()), 10)
	e.columnMapping = columnMapping
	e.hasLocalCache = localCacheLimit != ""
	if e.hasLocalCache {
		limit := 100000
		if localCacheLimit != DefaultPoolCode {
			userLimit, err := strconv.Atoi(localCacheLimit)
			if err != nil || userLimit <= 0 {
				return fmt.Errorf("invalid local cache limit for '%s'", e.t.String())
			}
			limit = userLimit
		}
		e.localCacheLimit = limit
	}
	e.redisCacheName = redisCacheName
	e.hasRedisCache = redisCacheName != ""
	e.cacheKey = cacheKey

	lazyList := e.getTag("custom_lazy_group", e.t.String(), "")
	if lazyList == "" {
		lazyList = flushLazyEventsList
	}
	e.lazyCacheKey = e.mysqlPoolCode + ":" + lazyList

	e.uniqueIndices = make(map[string][]string)
	for name, index := range uniqueIndices {
		e.uniqueIndices[name] = make([]string, len(index))
		for i := 1; i <= len(index); i++ {
			e.uniqueIndices[name][i-1] = index[i]
		}
	}
	return e.validateIndexes(uniqueIndices, indices)
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

func (e *entitySchema) getTagBool(field, key string) bool {
	tag := e.GetTag(field, key, "1", "")
	return tag == "1"
}

func (e *entitySchema) getFieldsQuery() string {
	return e.fieldsQuery
}

func (e *entitySchema) getStructureHash() string {
	return e.structureHash
}

func (e *entitySchema) getTags() map[string]map[string]string {
	return e.tags
}

func (e *entitySchema) uuid() uint64 {
	return (e.uuidServerID&255)<<56 + (codeStartTime << 24) + atomic.AddUint64(&e.uuidCounter, 1)
}

func (e *entitySchema) getForcedRedisCode() string {
	if e.hasRedisCache {
		return e.redisCacheName
	}
	return DefaultPoolCode
}

func (e *entitySchema) GetCacheKey() string {
	return e.cacheKey
}

func (e *entitySchema) DisableCache(local, redis bool) {
	if local {
		e.localCacheLimit = 0
		e.hasLocalCache = false
	}
	if redis {
		e.redisCacheName = ""
		e.hasRedisCache = false
	}
}

func (e *entitySchema) buildTableFields(t reflect.Type, registry *Registry,
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
		if f.Type.Kind().String() == "array" {
			attributes.IsArray = true
			attributes.ArrayLen = f.Type.Len()
			attributes.TypeName = f.Type.Elem().String()
		}

		switch attributes.TypeName {
		case "uint",
			"uint8",
			"uint16",
			"uint32",
			"uint64":
			e.buildUintField(attributes)
		case "*uint",
			"*uint8",
			"*uint16",
			"*uint32",
			"*uint64":
			e.buildUintPointerField(attributes)
		case "int",
			"int8",
			"int16",
			"int32",
			"int64":
			e.buildIntField(attributes)
		case "*int",
			"*int8",
			"*int16",
			"*int32",
			"*int64":
			e.buildIntPointerField(attributes)
		case "string":
			e.buildStringField(attributes)
		case "[]uint8":
			fields.bytes = append(fields.bytes, i)
			columnName := attributes.GetColumnName()
			e.columnAttrToStringSetters[columnName] = createNotSupportedColumnSetter(columnName)
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
				if fType.Kind().String() == "string" {
					e.buildEnumField(attributes, definition)
				} else {
					e.buildStringSliceField(attributes, definition)
				}
			} else if fType.Implements(reflect.TypeOf((*referenceInterface)(nil)).Elem()) {
				e.buildReferenceField(attributes)
				if attributes.Tags["cached"] == "true" {
					fields.forcedOldBid[i] = true
				}
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
	IsArray  bool
	ArrayLen int
}

func (attributes schemaFieldAttributes) GetColumnName() string {
	return attributes.Prefix + attributes.Field.Name
}

func (e *entitySchema) buildUintField(attributes schemaFieldAttributes) {
	columnNamePrefix := attributes.GetColumnName()
	if !attributes.IsArray {
		attributes.Fields.uIntegersArray = append(attributes.Fields.uIntegersArray, attributes.Index)
	} else {
		attributes.Fields.uIntegers = append(attributes.Fields.uIntegers, attributes.Index)
	}
	for i := 0; i <= attributes.ArrayLen; i++ {
		columnName := columnNamePrefix
		if attributes.IsArray {
			if i == attributes.ArrayLen {
				break
			}
			columnName += "_" + strconv.Itoa(i+1)

		}
		e.mapBindToScanPointer[columnName] = func() interface{} {
			v := uint64(0)
			return &v
		}
		e.mapPointerToValue[columnName] = func(val interface{}) interface{} {
			return *val.(*uint64)
		}
		e.columnAttrToStringSetters[columnName] = createNumberColumnSetter(columnName, true)
	}
}

func (e *entitySchema) buildReferenceField(attributes schemaFieldAttributes) {

	columnNamePrefix := attributes.GetColumnName()
	fType := attributes.Field.Type
	if attributes.IsArray {
		fType = fType.Elem()
	}
	for i := 0; i <= attributes.ArrayLen; i++ {
		columnName := columnNamePrefix
		if attributes.IsArray {
			if i == attributes.ArrayLen {
				break
			}
			columnName += "_" + strconv.Itoa(i+1)
			attributes.Fields.referencesArray = append(attributes.Fields.referencesArray, attributes.Index)
			attributes.Fields.referencesRequiredArray = append(attributes.Fields.referencesRequiredArray, attributes.Tags["required"] == "true")
		} else {
			attributes.Fields.references = append(attributes.Fields.references, attributes.Index)
			attributes.Fields.referencesRequired = append(attributes.Fields.referencesRequired, attributes.Tags["required"] == "true")
		}
		e.mapBindToScanPointer[columnName] = scanIntNullablePointer
		e.mapPointerToValue[columnName] = pointerUintNullableScan
		e.columnAttrToStringSetters[columnName] = createNumberColumnSetter(columnName, true)
		refType := reflect.New(fType.Elem()).Interface().(referenceInterface).getType()
		def := referenceDefinition{
			Cached: attributes.Tags["cached"] == "true",
			Type:   refType,
		}
		if def.Cached {
			e.cachedReferences[columnName] = def
		}
		e.references[columnName] = def
	}
}

func (e *entitySchema) buildUintPointerField(attributes schemaFieldAttributes) {
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
	e.mapBindToScanPointer[columnName] = scanIntNullablePointer
	e.mapPointerToValue[columnName] = pointerUintNullableScan
	e.columnAttrToStringSetters[columnName] = createNumberColumnSetter(columnName, true)
}

func (e *entitySchema) buildIntField(attributes schemaFieldAttributes) {
	attributes.Fields.integers = append(attributes.Fields.integers, attributes.Index)
	columnName := attributes.GetColumnName()
	e.mapBindToScanPointer[columnName] = func() interface{} {
		v := int64(0)
		return &v
	}
	e.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*int64)
	}
	e.columnAttrToStringSetters[columnName] = createNumberColumnSetter(columnName, false)
}

func (e *entitySchema) buildIntPointerField(attributes schemaFieldAttributes) {
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
	e.mapBindToScanPointer[columnName] = scanIntNullablePointer
	e.mapPointerToValue[columnName] = pointerIntNullableScan
	e.columnAttrToStringSetters[columnName] = createNumberColumnSetter(columnName, false)
}

func (e *entitySchema) buildEnumField(attributes schemaFieldAttributes, definition interface{}) {
	columnName := attributes.GetColumnName()
	attributes.Fields.stringsEnums = append(attributes.Fields.stringsEnums, attributes.Index)
	def := initEnumDefinition(definition, attributes.Tags["required"] == "true")
	attributes.Fields.enums = append(attributes.Fields.enums, def)
	e.mapBindToScanPointer[columnName] = func() interface{} {
		return &sql.NullString{}
	}
	e.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		v := val.(*sql.NullString)
		if v.Valid {
			return v.String
		}
		return nil
	}
	e.columnAttrToStringSetters[columnName] = createStringColumnSetter(columnName)
}

func (e *entitySchema) buildStringField(attributes schemaFieldAttributes) {
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
	e.mapBindToScanPointer[columnName] = func() interface{} {
		return &sql.NullString{}
	}
	e.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		v := val.(*sql.NullString)
		if v.Valid {
			return v.String
		}
		return nil
	}
	e.columnAttrToStringSetters[columnName] = createStringColumnSetter(columnName)
}

func (e *entitySchema) buildStringSliceField(attributes schemaFieldAttributes, definition interface{}) {
	columnName := attributes.GetColumnName()
	attributes.Fields.sliceStringsSets = append(attributes.Fields.sliceStringsSets, attributes.Index)
	attributes.Fields.sets = append(attributes.Fields.sets, initEnumDefinition(definition, attributes.Tags["required"] == "true"))
	e.mapBindToScanPointer[columnName] = scanStringNullablePointer
	e.mapPointerToValue[columnName] = pointerStringNullableScan
	e.columnAttrToStringSetters[columnName] = createNotSupportedColumnSetter(columnName)
}

func (e *entitySchema) buildBoolField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	attributes.Fields.booleans = append(attributes.Fields.booleans, attributes.Index)
	e.mapBindToScanPointer[columnName] = scanBoolPointer
	e.mapPointerToValue[columnName] = pointerBoolScan
	e.columnAttrToStringSetters[columnName] = createBoolColumnSetter(columnName)
}

func (e *entitySchema) buildBoolPointerField(attributes schemaFieldAttributes) {
	attributes.Fields.booleansNullable = append(attributes.Fields.booleansNullable, attributes.Index)
	columnName := attributes.GetColumnName()
	e.mapBindToScanPointer[columnName] = scanBoolNullablePointer
	e.mapPointerToValue[columnName] = pointerBoolNullableScan
	e.columnAttrToStringSetters[columnName] = createBoolColumnSetter(columnName)
}

func (e *entitySchema) buildFloatField(attributes schemaFieldAttributes) {
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
	e.mapBindToScanPointer[columnName] = func() interface{} {
		v := float64(0)
		return &v
	}
	e.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*float64)
	}
	e.columnAttrToStringSetters[columnName] = createNotSupportedColumnSetter(columnName)
}

func (e *entitySchema) buildFloatPointerField(attributes schemaFieldAttributes) {
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
	e.mapBindToScanPointer[columnName] = scanFloatNullablePointer
	e.mapPointerToValue[columnName] = pointerFloatNullableScan
	e.columnAttrToStringSetters[columnName] = createNotSupportedColumnSetter(columnName)
}

func (e *entitySchema) buildTimePointerField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	_, hasTime := attributes.Tags["time"]
	if hasTime {
		attributes.Fields.timesNullable = append(attributes.Fields.timesNullable, attributes.Index)
	} else {
		attributes.Fields.datesNullable = append(attributes.Fields.datesNullable, attributes.Index)
	}
	e.mapBindToScanPointer[columnName] = scanStringNullablePointer
	e.mapPointerToValue[columnName] = pointerStringNullableScan
	e.columnAttrToStringSetters[columnName] = createDateTimeColumnSetter(columnName, hasTime)
}

func (e *entitySchema) buildTimeField(attributes schemaFieldAttributes) {
	columnName := attributes.GetColumnName()
	_, hasTime := attributes.Tags["time"]
	if hasTime {
		attributes.Fields.times = append(attributes.Fields.times, attributes.Index)
	} else {
		attributes.Fields.dates = append(attributes.Fields.dates, attributes.Index)
	}
	e.mapBindToScanPointer[columnName] = scanStringPointer
	e.mapPointerToValue[columnName] = pointerStringScan
	e.columnAttrToStringSetters[columnName] = createDateTimeColumnSetter(columnName, hasTime)
}

func (e *entitySchema) buildStructField(attributes schemaFieldAttributes, registry *Registry,
	schemaTags map[string]map[string]string) {
	subPrefix := ""
	if !attributes.Field.Anonymous {
		subPrefix = attributes.Field.Name
	}
	if attributes.IsArray {
		attributes.Fields.structsArray = append(attributes.Fields.structsArray, attributes.Index)
		subFields := e.buildTableFields(attributes.Field.Type.Elem(), registry, 0, subPrefix, schemaTags)
		attributes.Fields.structsFieldsArray = append(attributes.Fields.structsFieldsArray, subFields)
	} else {
		attributes.Fields.structs = append(attributes.Fields.structs, attributes.Index)
		subFields := e.buildTableFields(attributes.Field.Type, registry, 0, subPrefix, schemaTags)
		attributes.Fields.structsFields = append(attributes.Fields.structsFields, subFields)
	}
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

func (e *entitySchema) getFields() *tableFields {
	return e.fields
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
