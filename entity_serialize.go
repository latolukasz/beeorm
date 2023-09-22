package beeorm

import (
	"math"
	"reflect"
	"time"
)

func serializeEntity(schema EntitySchema, elem reflect.Value, serializer *serializer) {
	serializer.SerializeUInteger(schema.getStructureHash())
	serializeFields(serializer, schema.getFields(), elem)
}

func serializeFields(serialized *serializer, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.uIntegers {
		v := elem.Field(i).Uint()
		serialized.SerializeUInteger(v)
	}
	for _, i := range fields.references {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeUInteger(0)
		} else {
			serialized.SerializeUInteger(f.Interface().(referenceInterface).GetID())
		}
	}
	for _, i := range fields.integers {
		serialized.SerializeInteger(elem.Field(i).Int())
	}
	for _, i := range fields.booleans {
		serialized.SerializeBool(elem.Field(i).Bool())
	}
	for k, i := range fields.floats {
		f := elem.Field(i).Float()
		p := math.Pow10(fields.floatsPrecision[k])
		serialized.SerializeFloat(math.Round(f*p) / p)
	}
	for _, i := range fields.times {
		serialized.SerializeInteger(elem.Field(i).Interface().(time.Time).Unix())
	}
	for _, i := range fields.dates {
		serialized.SerializeInteger(elem.Field(i).Interface().(time.Time).Unix())
	}
	for _, i := range fields.strings {
		serialized.SerializeString(elem.Field(i).String())
	}
	for _, i := range fields.uIntegersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			serialized.SerializeUInteger(f.Elem().Uint())
		}
	}
	for _, i := range fields.integersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			serialized.SerializeInteger(f.Elem().Int())
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		val := elem.Field(i).String()
		if val == "" {
			serialized.SerializeUInteger(0)
		} else {
			serialized.SerializeUInteger(uint64(fields.enums[k].Index(val)))
		}
		k++
	}
	for _, i := range fields.bytes {
		serialized.SerializeBytes(elem.Field(i).Bytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		f := elem.Field(i)
		l := f.Len()
		serialized.SerializeUInteger(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for j := 0; j < l; j++ {
				serialized.SerializeUInteger(uint64(set.Index(f.Index(j).String())))
			}
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			serialized.SerializeBool(f.Elem().Bool())
		}
	}
	for k, i := range fields.floatsNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			val := f.Elem().Float()
			p := math.Pow10(fields.floatsNullablePrecision[k])
			serialized.SerializeFloat(math.Round(val*p) / p)
		}
	}
	for _, i := range fields.timesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			serialized.SerializeInteger(f.Interface().(*time.Time).Unix())
		}
	}
	for _, i := range fields.datesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			serialized.SerializeInteger(f.Interface().(*time.Time).Unix())
		}
	}
	for j, i := range fields.structs {
		serializeFields(serialized, fields.structsFields[j], elem.Field(i))
	}
}
