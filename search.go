package beeorm

import (
	"database/sql"
	"reflect"
	"strconv"
)

func SearchWithCount[E any](c Context, where *Where, pager *Pager) (results []*E, totalRows int) {
	return search[E](c, where, pager, true)
}

func Search[E any](c Context, where *Where, pager *Pager) []*E {
	results, _ := search[E](c, where, pager, false)
	return results
}

func SearchIDsWithCount[E any](c Context, where *Where, pager *Pager) (results []uint64, totalRows int) {
	var entity E
	return searchIDs(c, reflect.TypeOf(entity), where, pager, true)
}

func SearchIDs[E any](c Context, where *Where, pager *Pager) []uint64 {
	var entity E
	ids, _ := searchIDs(c, reflect.TypeOf(entity), where, pager, false)
	return ids
}

func SearchOne[E any](c Context, where *Where) (entity *E, found bool) {
	return searchOne[E](c, where)
}

func prepareScan(schema EntitySchema) (pointers []interface{}) {
	count := len(schema.GetColumns())
	pointers = make([]interface{}, count)
	prepareScanForFields(schema.getFields(), 0, pointers)
	return pointers
}

func prepareScanForFields(fields *tableFields, start int, pointers []interface{}) int {
	for range fields.uIntegers {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for range fields.references {
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
	for range fields.uIntegersNullable {
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
	for _, subFields := range fields.structsFields {
		start = prepareScanForFields(subFields, start, pointers)
	}
	return start
}

func searchRow[E any](c Context, where *Where, entityToFill *E, isSearch bool) (entity *E, found bool) {
	schema := getEntitySchema[E](c)
	if isSearch {
		where = runPluginInterfaceEntitySearch(c, where, schema)
	}
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT " + schema.getFieldsQuery() + " FROM `" + schema.GetTableName() + "` WHERE " + whereQuery + " LIMIT 1"

	pool := schema.GetDB()
	results, def := pool.Query(c, query, where.GetParameters()...)
	defer def()
	if !results.Next() {
		return entity, false
	}
	pointers := prepareScan(schema)
	results.Scan(pointers...)
	def()
	var value reflect.Value
	if entityToFill != nil {
		entity = entityToFill
		value = reflect.ValueOf(entity)
	} else {
		value = reflect.New(schema.t)
		entity = value.Interface().(*E)
	}
	deserializeFromDB(schema.getFields(), value.Elem(), pointers)
	return entity, true
}

func runPluginInterfaceEntitySearch(c Context, where *Where, schema EntitySchema) *Where {
	for _, pluginCode := range c.Engine().Registry().Plugins() {
		plugin := c.Engine().Registry().Plugin(pluginCode)
		interfaceEntitySearch, isInterfaceEntitySearch := plugin.(PluginInterfaceEntitySearch)
		if isInterfaceEntitySearch {
			where = interfaceEntitySearch.PluginInterfaceEntitySearch(c, schema, where)
		}
	}
	return where
}

func search[E any](c Context, where *Where, pager *Pager, withCount bool) (results []*E, totalRows int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := getEntitySchema[E](c)
	entities := reflect.MakeSlice(schema.tSlice, 0, 0)
	where = runPluginInterfaceEntitySearch(c, where, schema)

	whereQuery := where.String()
	/* #nosec */
	query := "SELECT ID" + schema.getFieldsQuery() + " FROM `" + schema.GetTableName() + "` WHERE " + whereQuery + " " + pager.String()
	pool := schema.GetDB()
	queryResults, def := pool.Query(c, query, where.GetParameters()...)
	defer def()

	i := 0
	for queryResults.Next() {
		pointers := prepareScan(schema)
		queryResults.Scan(pointers...)
		value := reflect.New(schema.t)
		deserializeFromDB(schema.getFields(), value.Elem(), pointers)
		entities = reflect.Append(entities, value)
		i++
	}
	def()
	totalRows = getTotalRows(c, withCount, pager, where, schema, i)
	return entities.Interface().([]*E), totalRows
}

func searchOne[E any](c Context, where *Where) (entity *E, found bool) {
	return searchRow[E](c, where, nil, true)
}

func searchIDs(c Context, entity reflect.Type, where *Where, pager *Pager, withCount bool) (ids []uint64, total int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := c.Engine().Registry().EntitySchema(entity)
	where = runPluginInterfaceEntitySearch(c, where, schema)
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT `ID` FROM `" + schema.GetTableName() + "` WHERE " + whereQuery + " " + pager.String()
	pool := schema.GetDB()
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
			query := "SELECT count(1) FROM `" + schema.GetTableName() + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetDB()
			pool.QueryRow(c, NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}
