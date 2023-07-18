package beeorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

func SearchWithCount(c Context, where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int) {
	return search(c, where, pager, true, true, reflect.ValueOf(entities), references...)
}

func Search(c Context, where *Where, pager *Pager, entities interface{}, references ...string) {
	search(c, where, pager, false, true, reflect.ValueOf(entities), references...)
}

func SearchIDsWithCount[E Entity](c Context, where *Where, pager *Pager) (results []uint64, totalRows int) {
	return searchIDs[E](c, where, pager, true)
}

func SearchIDs[E Entity](c Context, where *Where, pager *Pager) []uint64 {
	ids, _ := searchIDs[E](c, where, pager, false)
	return ids
}

func SearchOne[E Entity](c Context, where *Where, references ...string) (entity E) {
	return searchOne[E](c, where, references)
}

func prepareScan(schema EntitySchema) (pointers []interface{}) {
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

func searchRow[E Entity](c Context, where *Where, entityToFill Entity, isSearch bool, references []string) (entity E) {
	schema := GetEntitySchema[E](c)
	if isSearch {
		where = runPluginInterfaceEntitySearch(c, where, schema)
	}
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT ID" + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT 1"

	pool := schema.GetMysql()
	results, def := pool.Query(c, query, where.GetParameters()...)
	defer def()
	if !results.Next() {
		return
	}
	pointers := prepareScan(schema)
	results.Scan(pointers...)
	def()
	if entityToFill != nil {
		entity = entityToFill.(E)
	} else {
		entity = schema.NewEntity().(E)
	}
	fillFromDBRow(c, schema, pointers, entity)
	//if len(references) > 0 {
	//	warmUpReferences(serializer, engine, schema, entity.getORM().value, references, false)
	//}
	return
}

func runPluginInterfaceEntitySearch(c Context, where *Where, schema EntitySchema) *Where {
	for _, plugin := range c.Engine().Registry().plugins {
		interfaceEntitySearch, isInterfaceEntitySearch := plugin.(PluginInterfaceEntitySearch)
		if isInterfaceEntitySearch {
			where = interfaceEntitySearch.PluginInterfaceEntitySearch(c, schema, where)
		}
	}
	return where
}

func search(c Context, where *Where, pager *Pager, withCount, checkIsSlice bool, entities reflect.Value, references ...string) (totalRows int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	entities.SetLen(0)
	schema, hasSchema, name := getEntitySchemaForSlice(c.Engine(), entities.Type(), checkIsSlice)
	if !hasSchema {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	where = runPluginInterfaceEntitySearch(c, where, schema)

	whereQuery := where.String()
	/* #nosec */
	query := "SELECT ID" + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " " + pager.String()
	pool := schema.GetMysql()
	results, def := pool.Query(c, query, where.GetParameters()...)
	defer def()

	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		pointers := prepareScan(schema)
		results.Scan(pointers...)
		entity := schema.NewEntity()
		fillFromDBRow(c, schema, pointers, entity)
		val = reflect.Append(val, reflect.ValueOf(entity))
		i++
	}
	def()
	totalRows = getTotalRows(c, withCount, pager, where, schema, i)
	//if len(references) > 0 && i > 0 {
	//	warmUpReferences(serializer, engine, schema, val, references, true)
	//}
	valOrigin.Set(val)
	return totalRows
}

func searchOne[E Entity](c Context, where *Where, references []string) E {
	return searchRow[E](c, where, nil, true, references)
}

func searchIDs[E Entity](c Context, where *Where, pager *Pager, withCount bool) (ids []uint64, total int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := GetEntitySchema[E](c)
	where = runPluginInterfaceEntitySearch(c, where, schema)
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT `ID` FROM `" + schema.tableName + "` WHERE " + whereQuery + " " + pager.String()
	pool := schema.GetMysql()
	results, def := pool.Query(c, query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0)
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := getTotalRows(c, withCount, pager, where, schema, len(result))
	return result, totalRows
}

func getTotalRows(c Context, withCount bool, pager *Pager, where *Where, schema EntitySchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := "SELECT count(1) FROM `" + schema.tableName + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetMysql()
			pool.QueryRow(c, NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}

func fillFromDBRow(c Context, schema EntitySchema, pointers []interface{}, entity Entity) {
	orm := initIfNeeded(schema, entity)
	orm.inDB = true
	orm.loaded = true
	s := c.getSerializer()
	s.Reset(nil)
	orm.deserializeFromDB(s, pointers)
	orm.deserialize(c)
}

func fillFromBinary(c Context, schema EntitySchema, binary []byte, entity Entity) {
	orm := initIfNeeded(schema, entity)
	orm.inDB = true
	orm.loaded = true
	orm.binary = binary
	orm.deserialize(c)
}

func getEntitySchemaForSlice(engine Engine, sliceType reflect.Type, checkIsSlice bool) (EntitySchema, bool, string) {
	name := sliceType.String()
	if name[0] == 42 {
		name = name[1:]
	}
	if name[0] == 91 {
		name = name[3:]
	} else if checkIsSlice {
		panic(fmt.Errorf("interface %s is not slice of beeorm.Entity", sliceType.String()))
	}
	t, has := engine.entities[name]
	if !has {
		return nil, false, name
	}
	return engine.entitySchemas[t], true, name
}
