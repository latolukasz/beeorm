package beeorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func deserializeFromDB(serializer *serializer, pointers []interface{}) {
	orm.deserializeStructFromDB(serializer, 0, orm.entitySchema.getFields(), pointers, true)
	orm.binary = serializer.Read()
}

func deserializeFromBinary(c Context) {
	s := c.getSerializer()
	s.Reset(orm.binary)
	hash := s.DeserializeUInteger()
	if hash != orm.entitySchema.getStructureHash() {
		panic(fmt.Errorf("%s entity cache data use wrong hash", orm.entitySchema.GetType().String()))
	}
	orm.deserializeFields(c, orm.entitySchema.getFields(), orm.elem)
	orm.loaded = true
}

func deserializeFields(c Context, fields *tableFields, elem reflect.Value) {
	serializer := c.getSerializer()
	for _, i := range fields.uintegers {
		v := serializer.DeserializeUInteger()
		elem.Field(i).SetUint(v)
	}
	k := 0
	for _, i := range fields.refs {
		id := serializer.DeserializeUInteger()
		f := elem.Field(i)
		isNil := f.IsNil()
		if id > 0 {
			e := c.Engine().Registry().EntitySchema(fields.refsTypes[k]).NewEntity()
			o := e.getORM()
			o.idElem.SetUint(id)
			o.inDB = true
			f.Set(o.value)
		} else if !isNil {
			elem.Field(i).Set(reflect.Zero(reflect.PtrTo(fields.refsTypes[k])))
		}
		k++
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(serializer.DeserializeInteger())
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(serializer.DeserializeBool())
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(serializer.DeserializeFloat())
	}
	for _, i := range fields.times {
		f := elem.Field(i)
		unix := serializer.DeserializeInteger()
		if unix == zeroDateSeconds {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix-timeStampSeconds, 0)))
		}
	}
	for _, i := range fields.dates {
		f := elem.Field(i)
		unix := serializer.DeserializeInteger()
		if unix == zeroDateSeconds {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix-timeStampSeconds, 0)))
		}
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(serializer.DeserializeString())
	}
	for k, i := range fields.uintegersNullable {
		if serializer.DeserializeBool() {
			v := serializer.DeserializeUInteger()
			switch fields.uintegersNullableSize[k] {
			case 0:
				val := uint(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 8:
				val := uint8(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 16:
				val := uint16(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 32:
				val := uint32(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 64:
				elem.Field(i).Set(reflect.ValueOf(&v))
			}
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			elem.Field(i).Set(reflect.Zero(f.Type()))
		}
	}
	for k, i := range fields.integersNullable {
		if serializer.DeserializeBool() {
			v := serializer.DeserializeInteger()
			switch fields.integersNullableSize[k] {
			case 0:
				val := int(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 8:
				val := int8(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 16:
				val := int16(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 32:
				val := int32(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 64:
				elem.Field(i).Set(reflect.ValueOf(&v))
			}
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			elem.Field(i).Set(reflect.Zero(f.Type()))
		}
	}
	for z, i := range fields.stringsEnums {
		index := serializer.DeserializeUInteger()
		if index == 0 {
			elem.Field(i).SetString("")
		} else {
			elem.Field(i).SetString(fields.enums[z].GetFields()[index-1])
		}
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes(serializer.DeserializeBytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		l := int(serializer.DeserializeUInteger())
		f := elem.Field(i)
		if l == 0 {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		} else {
			enum := fields.sets[k]
			v := make([]string, l)
			for j := 0; j < l; j++ {
				v[j] = enum.GetFields()[serializer.DeserializeUInteger()-1]
			}
			f.Set(reflect.ValueOf(v))
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		if serializer.DeserializeBool() {
			v := serializer.DeserializeBool()
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for k, i := range fields.floatsNullable {
		if serializer.DeserializeBool() {
			v := serializer.DeserializeFloat()
			if fields.floatsNullableSize[k] == 32 {
				val := float32(v)
				elem.Field(i).Set(reflect.ValueOf(&val))
			} else {
				elem.Field(i).Set(reflect.ValueOf(&v))
			}
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for _, i := range fields.timesNullable {
		if serializer.DeserializeBool() {
			v := time.Unix(serializer.DeserializeInteger()-timeStampSeconds, 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for _, i := range fields.datesNullable {
		if serializer.DeserializeBool() {
			v := time.Unix(serializer.DeserializeInteger()-timeStampSeconds, 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for k, i := range fields.structs {
		orm.deserializeFields(c, fields.structsFields[k], elem.Field(i))
	}
}

func deserializeStructFromDB(serializer *serializer, index int, fields *tableFields, pointers []interface{}, root bool) int {
	if root {
		serializer.SerializeUInteger(orm.entitySchema.getStructureHash())
	}
	for range fields.uintegers {
		serializer.SerializeUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.refs {
		v := pointers[index].(*sql.NullInt64)
		serializer.SerializeUInteger(uint64(v.Int64))
		index++
	}
	for range fields.integers {
		serializer.SerializeInteger(*pointers[index].(*int64))
		index++
	}
	for range fields.booleans {
		serializer.SerializeBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for range fields.floats {
		serializer.SerializeFloat(*pointers[index].(*float64))
		index++
	}
	for range fields.times {
		unix := *pointers[index].(*int64)
		serializer.SerializeInteger(unix)
		index++
	}
	for range fields.dates {
		unix := *pointers[index].(*int64)
		serializer.SerializeInteger(unix)
		index++
	}
	for range fields.strings {
		serializer.SerializeString(pointers[index].(*sql.NullString).String)
		index++
	}
	for range fields.uintegersNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SerializeBool(v.Valid)
		if v.Valid {
			serializer.SerializeUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SerializeBool(v.Valid)
		if v.Valid {
			serializer.SerializeInteger(v.Int64)
		}
		index++
	}
	k := 0
	for range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			serializer.SerializeUInteger(uint64(fields.enums[k].Index(v.String)))
		} else {
			serializer.SerializeUInteger(0)
		}
		index++
		k++
	}
	for range fields.bytes {
		serializer.SerializeBytes([]byte(pointers[index].(*sql.NullString).String))
		index++
	}
	k = 0
	for range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid && v.String != "" {
			values := strings.Split(v.String, ",")
			serializer.SerializeUInteger(uint64(len(values)))
			enum := fields.sets[k]
			for _, set := range values {
				serializer.SerializeUInteger(uint64(enum.Index(set)))
			}
		} else {
			serializer.SerializeUInteger(0)
		}
		k++
		index++
	}
	for range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		serializer.SerializeBool(v.Valid)
		if v.Valid {
			serializer.SerializeBool(v.Bool)
		}
		index++
	}
	for range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		serializer.SerializeBool(v.Valid)
		if v.Valid {
			serializer.SerializeFloat(v.Float64)
		}
		index++
	}
	for range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SerializeBool(v.Valid)
		if v.Valid {
			unix := v.Int64
			serializer.SerializeInteger(unix)
		}
		index++
	}
	for range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SerializeBool(v.Valid)
		if v.Valid {
			unix := v.Int64
			serializer.SerializeInteger(unix)
		}
		index++
	}
	for _, subField := range fields.structsFields {
		index = orm.deserializeStructFromDB(serializer, index, subField, pointers, false)
	}
	return index
}
