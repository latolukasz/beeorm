package beeorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

func searchIDsWithCount(engine *engineImplementation, where *Where, pager *Pager, entityType reflect.Type) (results []uint64, totalRows int) {
	return searchIDs(engine, where, pager, true, entityType)
}

func prepareScan(schema *entitySchema) (pointers []interface{}) {
	count := len(schema.columnNames)
	pointers = make([]interface{}, count)
	prepareScanForFields(schema.fields, 0, pointers)
	return pointers
}

func prepareScanForFields(fields *tableFields, start int, pointers []interface{}) int {
	for range fields.uintegers {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for range fields.refs {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for range fields.integers {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for range fields.booleans {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for range fields.floats {
		v := float64(0)
		pointers[start] = &v
		start++
	}
	for range fields.times {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for range fields.dates {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for range fields.strings {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for range fields.uintegersNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for range fields.integersNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for range fields.stringsEnums {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for range fields.bytes {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for range fields.sliceStringsSets {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for range fields.booleansNullable {
		v := sql.NullBool{}
		pointers[start] = &v
		start++
	}
	for range fields.floatsNullable {
		v := sql.NullFloat64{}
		pointers[start] = &v
		start++
	}
	for range fields.timesNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for range fields.datesNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for range fields.jsons {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, subFields := range fields.structsFields {
		start = prepareScanForFields(subFields, start, pointers)
	}
	return start
}

func searchRow(serializer *serializer, engine *engineImplementation, where *Where, entity Entity, isSearch bool, references []string) (bool, *entitySchema, []interface{}) {
	orm := initIfNeeded(engine.registry, entity)
	schema := orm.entitySchema
	if isSearch {
		where = runPluginInterfaceEntitySearch(engine, where, schema)
	}
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT ID" + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT 1"

	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	if !results.Next() {
		return false, schema, nil
	}
	pointers := prepareScan(schema)
	results.Scan(pointers...)
	def()
	fillFromDBRow(serializer, engine.registry, pointers, entity)
	if len(references) > 0 {
		warmUpReferences(serializer, engine, schema, entity.getORM().value, references, false)
	}
	return true, schema, pointers
}

func runPluginInterfaceEntitySearch(engine *engineImplementation, where *Where, schema *entitySchema) *Where {
	for _, plugin := range engine.registry.plugins {
		interfaceEntitySearch, isInterfaceEntitySearch := plugin.(PluginInterfaceEntitySearch)
		if isInterfaceEntitySearch {
			where = interfaceEntitySearch.PluginInterfaceEntitySearch(engine, schema, where)
		}
	}
	return where
}

func search(serializer *serializer, engine *engineImplementation, where *Where, pager *Pager, withCount, checkIsSlice bool, entities reflect.Value, references ...string) (totalRows int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	entities.SetLen(0)
	entityType, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), checkIsSlice)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := getEntitySchema(engine.registry, entityType)
	where = runPluginInterfaceEntitySearch(engine, where, schema)

	whereQuery := where.String()
	/* #nosec */
	query := "SELECT ID" + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " " + pager.String()
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()

	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		pointers := prepareScan(schema)
		results.Scan(pointers...)
		value := reflect.New(entityType)
		fillFromDBRow(serializer, engine.registry, pointers, value.Interface().(Entity))
		val = reflect.Append(val, value)
		i++
	}
	def()
	totalRows = getTotalRows(engine, withCount, pager, where, schema, i)
	if len(references) > 0 && i > 0 {
		warmUpReferences(serializer, engine, schema, val, references, true)
	}
	valOrigin.Set(val)
	return totalRows
}

func searchOne(serializer *serializer, engine *engineImplementation, where *Where, entity Entity, references []string) (bool, *entitySchema, []interface{}) {
	return searchRow(serializer, engine, where, entity, true, references)
}

func searchIDs(engine *engineImplementation, where *Where, pager *Pager, withCount bool, entityType reflect.Type) (ids []uint64, total int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := getEntitySchema(engine.registry, entityType)
	where = runPluginInterfaceEntitySearch(engine, where, schema)
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT `ID` FROM `" + schema.tableName + "` WHERE " + whereQuery + " " + pager.String()
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0)
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := getTotalRows(engine, withCount, pager, where, schema, len(result))
	return result, totalRows
}

func getTotalRows(engine *engineImplementation, withCount bool, pager *Pager, where *Where, schema *entitySchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := "SELECT count(1) FROM `" + schema.tableName + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetMysql(engine)
			pool.QueryRow(NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}

func fillFromDBRow(serializer *serializer, registry *validatedRegistry, pointers []interface{}, entity Entity) {
	orm := initIfNeeded(registry, entity)
	orm.inDB = true
	orm.loaded = true
	orm.deserializeFromDB(serializer, pointers)
	orm.deserialize(serializer)
}

func fillFromBinary(serializer *serializer, registry *validatedRegistry, binary []byte, entity Entity) {
	orm := initIfNeeded(registry, entity)
	orm.inDB = true
	orm.loaded = true
	orm.binary = binary
	orm.deserialize(serializer)
}

func getEntityTypeForSlice(registry *validatedRegistry, sliceType reflect.Type, checkIsSlice bool) (reflect.Type, bool, string) {
	name := sliceType.String()
	if name[0] == 42 {
		name = name[1:]
	}
	if name[0] == 91 {
		name = name[3:]
	} else if checkIsSlice {
		panic(fmt.Errorf("interface %s is no slice of beeorm.Entity", sliceType.String()))
	}
	e, has := registry.entities[name]
	return e, has, name
}
