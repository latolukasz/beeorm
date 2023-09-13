package beeorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func deserializeFromDB(fields *tableFields, elem reflect.Value, pointers []interface{}) {
	deserializeStructFromDB(elem, 0, fields, pointers)
}

func deserializeFromBinary(s *serializer, schema EntitySchema, elem reflect.Value) {
	hash := s.DeserializeUInteger()
	if hash != schema.getStructureHash() {
		panic(fmt.Errorf("%s entity cache data use wrong hash", schema.GetType().String()))
	}
	deserializeFields(s, schema.getFields(), elem)
}

func deserializeFields(s *serializer, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.uintegers {
		v := s.DeserializeUInteger()
		elem.Field(i).SetUint(v)
	}
	k := 0
	for _, i := range fields.refs {
		id := s.DeserializeUInteger()
		f := elem.Field(i)
		isNil := f.IsNil()
		if id > 0 {
			e := reflect.New(fields.refsTypes[k])
			f.Set(e)
		} else if !isNil {
			elem.Field(i).Set(reflect.Zero(reflect.PtrTo(fields.refsTypes[k])))
		}
		k++
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(s.DeserializeInteger())
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(s.DeserializeBool())
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(s.DeserializeFloat())
	}
	for _, i := range fields.times {
		f := elem.Field(i)
		unix := s.DeserializeInteger()
		if unix == zeroDateSeconds {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix-timeStampSeconds, 0)))
		}
	}
	for _, i := range fields.dates {
		f := elem.Field(i)
		unix := s.DeserializeInteger()
		if unix == zeroDateSeconds {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix-timeStampSeconds, 0)))
		}
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(s.DeserializeString())
	}
	for k, i := range fields.uintegersNullable {
		if s.DeserializeBool() {
			v := s.DeserializeUInteger()
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
		if s.DeserializeBool() {
			v := s.DeserializeInteger()
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
		index := s.DeserializeUInteger()
		if index == 0 {
			elem.Field(i).SetString("")
		} else {
			elem.Field(i).SetString(string(fields.enums[z].GetFields()[index-1]))
		}
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes(s.DeserializeBytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		l := int(s.DeserializeUInteger())
		f := elem.Field(i)
		if l == 0 {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		} else {
			enum := fields.sets[k]
			v := make([]string, l)
			for j := 0; j < l; j++ {
				v[j] = enum.GetFields()[s.DeserializeUInteger()-1]
			}
			f.Set(reflect.ValueOf(v))
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		if s.DeserializeBool() {
			v := s.DeserializeBool()
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for k, i := range fields.floatsNullable {
		if s.DeserializeBool() {
			v := s.DeserializeFloat()
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
		if s.DeserializeBool() {
			v := time.Unix(s.DeserializeInteger()-timeStampSeconds, 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for _, i := range fields.datesNullable {
		if s.DeserializeBool() {
			v := time.Unix(s.DeserializeInteger()-timeStampSeconds, 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for k, i := range fields.structs {
		deserializeFields(s, fields.structsFields[k], elem.Field(i))
	}
}

func deserializeStructFromDB(elem reflect.Value, index int, fields *tableFields, pointers []interface{}) int {
	for _, i := range fields.uintegers {
		elem.Field(i).SetUint(*pointers[index].(*uint64))
		index++
	}
	for _, i := range fields.refs {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			f := elem.Field(i)
			ref := reflect.New(f.Type())
			//ref.Interface().(Entity).SetID(uint64(v.Int64))
			f.Set(ref)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(*pointers[index].(*int64))
		index++
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(*pointers[index].(*int64) > 0)
		index++
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(*pointers[index].(*float64))
		index++
	}
	for _, i := range fields.times {
		elem.Field(i).Set(reflect.ValueOf(time.Unix(*pointers[index].(*int64), 0)))
		index++
	}
	for _, i := range fields.dates {
		elem.Field(i).Set(reflect.ValueOf(time.Unix(*pointers[index].(*int64), 0)))
		index++
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(pointers[index].(*sql.NullString).String)
		index++
	}
	for _, i := range fields.uintegersNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			elem.Field(i).SetUint(uint64(v.Int64))
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			elem.Field(i).SetInt(v.Int64)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		elem.Field(i).SetString(v.String)
		index++
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes([]byte(pointers[index].(*sql.NullString).String))
		index++
	}
	for _, i := range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid && v.String != "" {
			values := strings.Split(v.String, ",")
			elem.Field(i).Set(reflect.ValueOf(values))
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		if v.Valid {
			elem.Field(i).SetBool(v.Bool)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		if v.Valid {
			elem.Field(i).SetFloat(v.Float64)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			elem.Field(i).Set(reflect.ValueOf(time.Unix(v.Int64, 0)))
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			elem.Field(i).Set(reflect.ValueOf(time.Unix(v.Int64, 0)))
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}

	for k, i := range fields.structs {
		index = deserializeStructFromDB(elem.Field(i), index, fields.structsFields[k], pointers)
	}
	return index
}
