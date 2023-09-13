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
	for _, i := range fields.uintegers {
		v := elem.Field(i).Uint()
		serialized.SerializeUInteger(v)
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
		t := elem.Field(i).Interface().(time.Time)
		if t.IsZero() {
			serialized.SerializeInteger(zeroDateSeconds)
		} else {
			unix := t.Unix()
			if unix > 0 {
				unix += timeStampSeconds
			} else {
				unix = zeroDateSeconds
			}
			serialized.SerializeInteger(unix)
		}
	}
	for _, i := range fields.dates {
		t := elem.Field(i).Interface().(time.Time)
		if t.IsZero() {
			serialized.SerializeInteger(zeroDateSeconds)
		} else {
			unix := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
			if unix > 0 {
				unix += timeStampSeconds
			} else {
				unix = zeroDateSeconds
			}
			serialized.SerializeInteger(unix)
		}
	}
	for _, i := range fields.strings {
		serialized.SerializeString(elem.Field(i).String())
	}
	for _, i := range fields.uintegersNullable {
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
		values := f.Interface().([]string)
		l := len(values)
		serialized.SerializeUInteger(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for _, val := range values {
				serialized.SerializeUInteger(uint64(set.Index(val)))
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
			unix := f.Interface().(*time.Time).Unix()
			if unix > 0 {
				unix += timeStampSeconds
			} else {
				unix = zeroDateSeconds
			}
			serialized.SerializeInteger(unix)
		}
	}
	for _, i := range fields.datesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBool(false)
		} else {
			serialized.SerializeBool(true)
			t := f.Interface().(*time.Time)
			unix := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
			if unix > 0 {
				unix += timeStampSeconds
			} else {
				unix = zeroDateSeconds
			}
			serialized.SerializeInteger(unix)
		}
	}
	for k, i := range fields.structs {
		serializeFields(serialized, fields.structsFields[k], elem.Field(i))
	}
}
