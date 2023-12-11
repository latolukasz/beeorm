package beeorm

import (
	"reflect"
)

func Copy[E any](c Context, source E) E {
	schema := c.Engine().Registry().EntitySchema(source).(*entitySchema)
	insertable := newEntityInsertable(c, schema)
	copyEntity(reflect.ValueOf(source).Elem(), insertable.value.Elem(), schema.fields, false)
	return insertable.entity.(E)
}

func copyToEdit[E any](c Context, source E) *editableEntity[E] {
	schema := getEntitySchema[E](c)
	value := reflect.New(schema.t)
	writable := &editableEntity[E]{}
	writable.c = c
	writable.schema = schema
	writable.entity = value.Interface().(E)
	writable.value = value
	writable.sourceValue = reflect.ValueOf(source)
	copyEntity(writable.sourceValue.Elem(), value.Elem(), schema.fields, true)
	return writable
}

func copyEntity(source, target reflect.Value, fields *tableFields, withID bool) {
	if withID {
		for _, i := range fields.uIntegers {
			target.Field(i).SetUint(source.Field(i).Uint())
		}
	} else {
		for _, i := range fields.uIntegers[1:] {
			target.Field(i).SetUint(source.Field(i).Uint())
		}
	}
	for _, i := range fields.uIntegersArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fTarget.Index(j).SetUint(fSource.Index(j).Uint())
		}
	}
	for _, i := range fields.references {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.referencesArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.integers {
		target.Field(i).SetInt(source.Field(i).Int())
	}
	for _, i := range fields.integersArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fTarget.Index(j).SetInt(fSource.Index(j).Int())
		}
	}
	for _, i := range fields.booleans {
		target.Field(i).SetBool(source.Field(i).Bool())
	}
	for _, i := range fields.booleansArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fTarget.Index(j).SetBool(fSource.Index(j).Bool())
		}
	}
	for _, i := range fields.floats {
		target.Field(i).SetFloat(source.Field(i).Float())
	}
	for _, i := range fields.floatsArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fTarget.Index(j).SetFloat(fSource.Index(j).Float())
		}
	}
	for _, i := range fields.times {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.timesArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.dates {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.datesArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.strings {
		target.Field(i).SetString(source.Field(i).String())
	}
	for _, i := range fields.stringsArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fTarget.Index(j).SetString(fSource.Index(j).String())
		}
	}
	for _, i := range fields.uIntegersNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.uIntegersNullableArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.integersNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.integersNullableArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.stringsEnums {
		target.Field(i).SetString(source.Field(i).String())
	}
	for _, i := range fields.stringsEnumsArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fTarget.Index(j).SetString(fSource.Index(j).String())
		}
	}
	for _, i := range fields.bytes {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.bytesArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.sliceStringsSets {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.sliceStringsSetsArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.booleansNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.booleansNullableArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.floatsNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.floatsNullableArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.timesNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.timesNullableArray {
		copyField(source, target, fields, i)
	}
	for _, i := range fields.datesNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.datesNullableArray {
		copyField(source, target, fields, i)
	}
	for k, i := range fields.structs {
		copyEntity(source.Field(i), target.Field(i), fields.structsFields[k], true)
	}
	for k, i := range fields.structsArray {
		fTarget := target.Field(i)
		fSource := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			copyEntity(fSource.Index(j), fTarget.Index(j), fields.structsFieldsArray[k], true)
		}
	}
}

func copyField(source reflect.Value, target reflect.Value, fields *tableFields, i int) {
	fTarget := target.Field(i)
	fSource := source.Field(i)
	for j := 0; j < fields.arrays[i]; j++ {
		fTarget.Index(j).Set(fSource.Index(j))
	}
}
