package beeorm

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func deserializeFromDB(fields *tableFields, elem reflect.Value, pointers []interface{}) {
	deserializeStructFromDB(elem, 0, fields, pointers)
}

func deserializeFromRedis(data []string, schema EntitySchema, elem reflect.Value) bool {
	hash := data[0]
	if hash != schema.getStructureHash() {
		return false
	}
	deserializeFieldsFromRedis(data, schema.getFields(), elem, 1)
	return true
}

func deserializeFieldsFromRedis(data []string, fields *tableFields, elem reflect.Value, index int) int {
	for _, i := range fields.uIntegers {
		v := data[index]
		index++
		if v == "" {
			elem.Field(i).SetUint(0)
		} else {
			val, _ := strconv.ParseUint(v, 10, 64)
			elem.Field(i).SetUint(val)
		}
	}
	for _, i := range fields.references {
		v := data[index]
		index++
		if v == "" {
			elem.Field(i).SetZero()
		} else {
			f := elem.Field(i)
			val := reflect.New(f.Type().Elem())
			reference := val.Interface().(referenceInterface)
			valInt, _ := strconv.ParseUint(v, 10, 64)
			reference.SetID(valInt)
			f.Set(val)
		}
	}
	for _, i := range fields.integers {
		v := data[index]
		index++
		if v == "" {
			elem.Field(i).SetInt(0)
		} else {
			val, _ := strconv.ParseInt(v, 10, 64)
			elem.Field(i).SetInt(val)
		}
	}
	for _, i := range fields.booleans {
		v := data[index]
		index++
		elem.Field(i).SetBool(v == "1")
	}
	for _, i := range fields.floats {
		v := data[index]
		index++
		if v == "" {
			elem.Field(i).SetFloat(0)
		} else {
			val, _ := strconv.ParseFloat(v, 64)
			elem.Field(i).SetFloat(val)
		}
	}
	for _, i := range fields.times {
		v := data[index]
		index++
		f := elem.Field(i)
		if v != "" {
			t, _ := time.ParseInLocation(time.DateTime, v, time.UTC)
			f.Set(reflect.ValueOf(t))
		} else {
			f.SetZero()
		}
	}
	for _, i := range fields.dates {
		v := data[index]
		index++
		f := elem.Field(i)
		if v != "" {
			t, _ := time.ParseInLocation(time.DateOnly, v, time.UTC)
			f.Set(reflect.ValueOf(t))
		} else {
			f.SetZero()
		}
	}
	for _, i := range fields.strings {
		v := data[index]
		index++
		elem.Field(i).SetString(v)
	}
	for k, i := range fields.uIntegersNullable {
		v := data[index]
		index++
		if v != "" {
			asInt, _ := strconv.ParseUint(v, 10, 64)
			switch fields.uIntegersNullableSize[k] {
			case 0:
				val := uint(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 8:
				val := uint8(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 16:
				val := uint16(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 32:
				val := uint32(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 64:
				elem.Field(i).Set(reflect.ValueOf(&asInt))
			}
			continue
		}
		elem.Field(i).SetZero()
	}
	for k, i := range fields.integersNullable {
		v := data[index]
		index++
		if v != "" {
			asInt, _ := strconv.ParseInt(v, 10, 64)
			switch fields.integersNullableSize[k] {
			case 0:
				val := int(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 8:
				val := int8(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 16:
				val := int16(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 32:
				val := int32(asInt)
				elem.Field(i).Set(reflect.ValueOf(&val))
			case 64:
				elem.Field(i).Set(reflect.ValueOf(&asInt))
			}
			continue
		}
		elem.Field(i).SetZero()
	}
	for _, i := range fields.stringsEnums {
		elem.Field(i).SetString(data[index])
		index++
	}
	for _, i := range fields.bytes {
		v := data[index]
		index++
		if v == "" {
			elem.Field(i).SetZero()
		} else {
			elem.Field(i).SetBytes([]byte(v))
		}
	}
	for _, i := range fields.sliceStringsSets {
		v := data[index]
		index++
		f := elem.Field(i)
		if v != "" {
			values := strings.Split(v, ",")
			l := len(values)
			newSlice := reflect.MakeSlice(f.Type(), l, l)
			for j, val := range values {
				newSlice.Index(j).SetString(val)
			}
			f.Set(newSlice)
		} else {
			f.SetZero()
		}
	}
	for _, i := range fields.booleansNullable {
		v := data[index]
		index++
		if v == "" {
			elem.Field(i).SetZero()
		} else {
			b := v == "1"
			elem.Field(i).Set(reflect.ValueOf(&b))
		}
	}
	for j, i := range fields.floatsNullable {
		v := data[index]
		index++
		if v != "" {
			asFloat, _ := strconv.ParseFloat(v, 64)
			if fields.floatsNullableSize[j] == 32 {
				val := float32(asFloat)
				elem.Field(i).Set(reflect.ValueOf(&val))
			} else {
				elem.Field(i).Set(reflect.ValueOf(&asFloat))
			}
			continue
		}
		elem.Field(i).SetZero()
	}
	for _, i := range fields.timesNullable {
		v := data[index]
		index++
		if v != "" {
			t, _ := time.ParseInLocation(time.DateTime, v, time.UTC)
			elem.Field(i).Set(reflect.ValueOf(&t))
		} else {
			elem.Field(i).SetZero()
		}
	}
	for _, i := range fields.datesNullable {
		v := data[index]
		index++
		if v != "" {
			t, _ := time.ParseInLocation(time.DateOnly, v, time.UTC)
			elem.Field(i).Set(reflect.ValueOf(&t))
		} else {
			elem.Field(i).SetZero()
		}
	}
	for j, i := range fields.structs {
		index = deserializeFieldsFromRedis(data, fields.structsFields[j], elem.Field(i), index)
	}
	return index
}

func deserializeStructFromDB(elem reflect.Value, index int, fields *tableFields, pointers []interface{}) int {
	for _, i := range fields.uIntegers {
		elem.Field(i).SetUint(*pointers[index].(*uint64))
		index++
	}
	for _, i := range fields.references {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			f := elem.Field(i)
			val := reflect.New(f.Type().Elem())
			reference := val.Interface().(referenceInterface)
			reference.SetID(uint64(v.Int64))
			f.Set(val)
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
		elem.Field(i).SetBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(*pointers[index].(*float64))
		index++
	}
	for _, i := range fields.times {
		v := *pointers[index].(*int64)
		if v == zeroDateSeconds {
			elem.Field(i).SetZero()
		} else {
			elem.Field(i).Set(reflect.ValueOf(time.Unix(v-timeStampSeconds, 0).UTC()))
		}
		index++
	}
	for _, i := range fields.dates {
		v := *pointers[index].(*int64)
		if v == zeroDateSeconds {
			elem.Field(i).SetZero()
		} else {
			elem.Field(i).Set(reflect.ValueOf(time.Unix(v-timeStampSeconds, 0).UTC()))
		}
		index++
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(pointers[index].(*sql.NullString).String)
		index++
	}
	for _, i := range fields.uIntegersNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			f := elem.Field(i)
			val := reflect.New(f.Type().Elem())
			val.Elem().SetUint(uint64(v.Int64))
			f.Set(val)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			f := elem.Field(i)
			val := reflect.New(f.Type().Elem())
			val.Elem().SetInt(v.Int64)
			f.Set(val)
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
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			elem.Field(i).SetBytes([]byte(v.String))
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid && v.String != "" {
			f := elem.Field(i)
			values := strings.Split(v.String, ",")
			setValues := reflect.MakeSlice(f.Type(), len(values), len(values))
			for j, val := range strings.Split(v.String, ",") {
				setValues.Index(j).SetString(val)
			}
			f.Set(setValues)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		if v.Valid {
			f := elem.Field(i)
			val := reflect.New(f.Type().Elem())
			val.Elem().SetBool(v.Bool)
			f.Set(val)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		if v.Valid {
			f := elem.Field(i)
			val := reflect.New(f.Type().Elem())
			val.Elem().SetFloat(v.Float64)
			f.Set(val)
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			if v.Int64 == zeroDateSeconds {
				elem.Field(i).SetZero()
			} else {
				t := time.Unix(v.Int64-timeStampSeconds, 0).UTC()
				elem.Field(i).Set(reflect.ValueOf(&t))
			}
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		if v.Valid {
			if v.Int64 == zeroDateSeconds {
				elem.Field(i).SetZero()
			} else {
				t := time.Unix(v.Int64-timeStampSeconds, 0).UTC()
				elem.Field(i).Set(reflect.ValueOf(&t))
			}
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
