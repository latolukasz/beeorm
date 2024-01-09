package beeorm

import (
	"database/sql"
	"reflect"
	"strconv"
)

func SearchWithCount[E any](orm ORM, where Where, pager *Pager) (results EntityIterator[E], totalRows int) {
	return search[E](orm, where, pager, true)
}

func Search[E any](orm ORM, where Where, pager *Pager) EntityIterator[E] {
	results, _ := search[E](orm, where, pager, false)
	return results
}

func SearchIDsWithCount[E any](orm ORM, where Where, pager *Pager) (results []uint64, totalRows int) {
	return searchIDs(orm, GetEntitySchema[E](orm), where, pager, true)
}

func SearchIDs[E any](orm ORM, where Where, pager *Pager) []uint64 {
	ids, _ := searchIDs(orm, GetEntitySchema[E](orm), where, pager, false)
	return ids
}

func SearchOne[E any](orm ORM, where Where) (entity *E, found bool) {
	return searchOne[E](orm, where)
}

func prepareScan(schema *entitySchema) (pointers []any) {
	count := len(schema.GetColumns())
	pointers = make([]any, count)
	prepareScanForFields(schema.fields, 0, pointers)
	return pointers
}

func prepareScanForFields(fields *tableFields, start int, pointers []any) int {
	for range fields.uIntegers {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.uIntegersArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := uint64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.references {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.referencesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullInt64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.integers {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.integersArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := int64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.booleans {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.booleansArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := uint64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.floats {
		v := float64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.floatsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := float64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.times {
		v := ""
		pointers[start] = &v
		start++
	}
	for _, i := range fields.timesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := ""
			pointers[start] = &v
			start++
		}
	}
	for range fields.dates {
		v := ""
		pointers[start] = &v
		start++
	}
	for _, i := range fields.datesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := ""
			pointers[start] = &v
			start++
		}
	}
	for range fields.strings {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.stringsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.uIntegersNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.uIntegersNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullInt64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.integersNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.integersNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullInt64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.stringsEnums {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.stringsEnumsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.bytes {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.bytesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.sliceStringsSets {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.sliceStringsSetsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.booleansNullable {
		v := sql.NullBool{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.booleansNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullBool{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.floatsNullable {
		v := sql.NullFloat64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.floatsNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullFloat64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.timesNullable {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.timesNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.datesNullable {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.datesNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for _, subFields := range fields.structsFields {
		start = prepareScanForFields(subFields, start, pointers)
	}
	for k, i := range fields.structsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			start = prepareScanForFields(fields.structsFieldsArray[k], start, pointers)
		}
	}
	return start
}

func searchRow[E any](orm ORM, where Where) (entity *E, found bool) {
	schema := getEntitySchema[E](orm)
	pool := schema.GetDB()
	whereQuery := where.String()

	if schema.hasLocalCache {
		query := "SELECT ID FROM `" + schema.GetTableName() + "` WHERE " + whereQuery + " LIMIT 1"
		var id uint64
		if pool.QueryRow(orm, NewWhere(query, where.GetParameters()...), &id) {
			return GetByID[E](orm, id)
		}
		return nil, false
	}

	/* #nosec */
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE " + whereQuery + " LIMIT 1"
	pointers := prepareScan(schema)
	found = pool.QueryRow(orm, NewWhere(query, where.GetParameters()...), pointers...)
	if !found {
		return nil, false
	}
	value := reflect.New(schema.t)
	entity = value.Interface().(*E)
	deserializeFromDB(schema.fields, value.Elem(), pointers)
	return entity, true
}

func search[E any](orm ORM, where Where, pager *Pager, withCount bool) (results EntityIterator[E], totalRows int) {
	schema := getEntitySchema[E](orm)
	entities := make([]*E, 0)
	if schema.hasLocalCache {
		ids, total := SearchIDsWithCount[E](orm, where, pager)
		if total == 0 {
			return &emptyResultsIterator[E]{}, 0
		}
		return &localCacheIDsIterator[E]{orm: orm.(*ormImplementation), schema: schema, ids: ids, index: -1}, total
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
		entities = append(entities, value.Interface().(*E))
		i++
	}
	def()
	totalRows = i
	if pager != nil {
		totalRows = getTotalRows(orm, withCount, pager, where, schema, i)
	}
	resultsIterator := &entityIterator[E]{index: -1}
	resultsIterator.rows = entities
	return resultsIterator, totalRows
}

func searchOne[E any](orm ORM, where Where) (*E, bool) {
	return searchRow[E](orm, where)
}

func searchIDs(orm ORM, schema EntitySchema, where Where, pager *Pager, withCount bool) (ids []uint64, total int) {
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT `ID` FROM `" + schema.GetTableName() + "` WHERE " + whereQuery
	if pager != nil {
		query += " " + pager.String()
	}
	pool := schema.GetDB()
	results, def := pool.Query(orm, query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0)
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := len(result)
	if pager != nil {
		totalRows = getTotalRows(orm, withCount, pager, where, schema, len(result))
	}
	return result, totalRows
}

func getTotalRows(orm ORM, withCount bool, pager *Pager, where Where, schema EntitySchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := "SELECT count(1) FROM `" + schema.GetTableName() + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetDB()
			pool.QueryRow(orm, NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}
