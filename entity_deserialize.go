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
	orm.binary = s.Read()
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
			elem.Field(i).SetString(fields.enums[z].GetFields()[index-1])
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

func deserializeStructFromDB(s *serializer, index int, fields *tableFields, pointers []interface{}, root bool) int {
	if root {
		s.SerializeUInteger(orm.entitySchema.getStructureHash())
	}
	for range fields.uintegers {
		s.SerializeUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.refs {
		v := pointers[index].(*sql.NullInt64)
		s.SerializeUInteger(uint64(v.Int64))
		index++
	}
	for range fields.integers {
		s.SerializeInteger(*pointers[index].(*int64))
		index++
	}
	for range fields.booleans {
		s.SerializeBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for range fields.floats {
		s.SerializeFloat(*pointers[index].(*float64))
		index++
	}
	for range fields.times {
		unix := *pointers[index].(*int64)
		s.SerializeInteger(unix)
		index++
	}
	for range fields.dates {
		unix := *pointers[index].(*int64)
		s.SerializeInteger(unix)
		index++
	}
	for range fields.strings {
		s.SerializeString(pointers[index].(*sql.NullString).String)
		index++
	}
	for range fields.uintegersNullable {
		v := pointers[index].(*sql.NullInt64)
		s.SerializeBool(v.Valid)
		if v.Valid {
			s.SerializeUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		s.SerializeBool(v.Valid)
		if v.Valid {
			s.SerializeInteger(v.Int64)
		}
		index++
	}
	k := 0
	for range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			s.SerializeUInteger(uint64(fields.enums[k].Index(v.String)))
		} else {
			s.SerializeUInteger(0)
		}
		index++
		k++
	}
	for range fields.bytes {
		s.SerializeBytes([]byte(pointers[index].(*sql.NullString).String))
		index++
	}
	k = 0
	for range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid && v.String != "" {
			values := strings.Split(v.String, ",")
			s.SerializeUInteger(uint64(len(values)))
			enum := fields.sets[k]
			for _, set := range values {
				s.SerializeUInteger(uint64(enum.Index(set)))
			}
		} else {
			s.SerializeUInteger(0)
		}
		k++
		index++
	}
	for range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		s.SerializeBool(v.Valid)
		if v.Valid {
			s.SerializeBool(v.Bool)
		}
		index++
	}
	for range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		s.SerializeBool(v.Valid)
		if v.Valid {
			s.SerializeFloat(v.Float64)
		}
		index++
	}
	for range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		s.SerializeBool(v.Valid)
		if v.Valid {
			unix := v.Int64
			s.SerializeInteger(unix)
		}
		index++
	}
	for range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		s.SerializeBool(v.Valid)
		if v.Valid {
			unix := v.Int64
			s.SerializeInteger(unix)
		}
		index++
	}
	for _, subField := range fields.structsFields {
		index = orm.deserializeStructFromDB(serializer, index, subField, pointers, false)
	}
	return index
}
