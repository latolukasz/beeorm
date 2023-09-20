package beeorm

import (
	"reflect"
)

func Copy[E Entity](c Context, source E) EditableEntity[E] {
	schema := GetEntitySchema[E](c).(*entitySchema)
	value := reflect.New(schema.GetType().Elem())
	writable := &editableEntity[E]{}
	writable.c = c
	writable.schema = schema
	writable.entity = value.Interface().(E)
	writable.value = value
	copyEntity(reflect.ValueOf(source).Elem(), value.Elem(), schema.fields)
	return writable
}

func copyEntity(source, target reflect.Value, fields *tableFields) {
	for _, i := range fields.uIntegers {
		target.Field(i).SetUint(source.Field(i).Uint())
	}
	for _, i := range fields.references {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.integers {
		target.Field(i).SetInt(source.Field(i).Int())
	}
	for _, i := range fields.booleans {
		target.Field(i).SetBool(source.Field(i).Bool())
	}
	for _, i := range fields.floats {
		target.Field(i).SetFloat(source.Field(i).Float())
	}
	for _, i := range fields.times {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.dates {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.strings {
		target.Field(i).SetString(source.Field(i).String())
	}
	for _, i := range fields.uIntegersNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.integersNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.stringsEnums {
		target.Field(i).SetString(source.Field(i).String())
	}
	for _, i := range fields.bytes {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.sliceStringsSets {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.booleansNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.floatsNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.timesNullable {
		target.Field(i).Set(source.Field(i))
	}
	for _, i := range fields.datesNullable {
		target.Field(i).Set(source.Field(i))
	}
	for k, i := range fields.structs {
		copyEntity(source.Field(i), target.Field(i), fields.structsFields[k])
	}
}
