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
		deserializeUintFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.uIntegersArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeUintFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.references {
		deserializeReferencesFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.referencesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeReferencesFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.integers {
		deserializeIntFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.integersArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeIntFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.booleans {
		deserializeBoolFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.booleansArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeBoolFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.floats {
		deserializeFloatFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.floatsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeFloatFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.times {
		deserializeTimeFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.timesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeTimeFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.dates {
		deserializeDateFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.datesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeDateFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.strings {
		deserializeStringFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.stringsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeStringFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for k, i := range fields.uIntegersNullable {
		deserializeUIntegersPointersFromRedis(data[index], elem.Field(i), fields.uIntegersNullableSize[k])
		index++
	}
	for k, i := range fields.uIntegersNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeUIntegersPointersFromRedis(data[index], f.Index(j), fields.uIntegersNullableSizeArray[k])
			index++
		}
	}
	for k, i := range fields.integersNullable {
		deserializeIntegersPointersFromRedis(data[index], elem.Field(i), fields.integersNullableSize[k])
		index++
	}
	for k, i := range fields.integersNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeIntegersPointersFromRedis(data[index], f.Index(j), fields.integersNullableSizeArray[k])
			index++
		}
	}
	for _, i := range fields.stringsEnums {
		deserializeStringFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.stringsEnumsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeStringFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.bytes {
		deserializeBytesFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.bytesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeBytesFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.sliceStringsSets {
		deserializeSliceStringFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.sliceStringsSetsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeSliceStringFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.booleansNullable {
		deserializeBoolPointersFromRedis(data[index], elem.Field(i))
		index++

	}
	for _, i := range fields.booleansNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeBoolPointersFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for j, i := range fields.floatsNullable {
		deserializeFloatPointersFromRedis(data[index], elem.Field(i), fields.floatsNullableSize[j])
		index++
	}
	for _, i := range fields.floatsNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeFloatPointersFromRedis(data[index], f.Index(j), fields.floatsNullableSizeArray[j])
			index++
		}
	}
	for _, i := range fields.timesNullable {
		deserializeTimePointersFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.timesNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeTimePointersFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for _, i := range fields.datesNullable {
		deserializeDatePointersFromRedis(data[index], elem.Field(i))
		index++
	}
	for _, i := range fields.datesNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			deserializeDatePointersFromRedis(data[index], f.Index(j))
			index++
		}
	}
	for j, i := range fields.structs {
		index = deserializeFieldsFromRedis(data, fields.structsFields[j], elem.Field(i), index)
	}
	for k, i := range fields.structsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			index = deserializeFieldsFromRedis(data, fields.structsFieldsArray[k], f.Index(j), index)
		}
	}
	return index
}

func deserializeUintFromRedis(v string, f reflect.Value) {
	if v == "" {
		f.SetUint(0)
	} else {
		val, _ := strconv.ParseUint(v, 10, 64)
		f.SetUint(val)
	}
}

func deserializeReferencesFromRedis(v string, f reflect.Value) {
	if v == "" {
		f.SetZero()
	} else {
		val := reflect.New(f.Type().Elem())
		reference := val.Interface().(referenceInterface)
		valInt, _ := strconv.ParseUint(v, 10, 64)
		reference.SetID(valInt)
		f.Set(val)
	}
}

func deserializeIntFromRedis(v string, f reflect.Value) {
	if v == "" {
		f.SetInt(0)
	} else {
		val, _ := strconv.ParseInt(v, 10, 64)
		f.SetInt(val)
	}
}

func deserializeBoolFromRedis(v string, f reflect.Value) {
	f.SetBool(v != zeroAsString)
}

func deserializeFloatFromRedis(v string, f reflect.Value) {
	if v == "" {
		f.SetFloat(0)
	} else {
		val, _ := strconv.ParseFloat(v, 64)
		f.SetFloat(val)
	}
}

func deserializeTimeFromRedis(v string, f reflect.Value) {
	if v != "" {
		t, _ := time.ParseInLocation(time.DateTime, v, time.UTC)
		f.Set(reflect.ValueOf(t))
	} else {
		f.SetZero()
	}
}

func deserializeDateFromRedis(v string, f reflect.Value) {
	if v != "" {
		t, _ := time.ParseInLocation(time.DateOnly, v, time.UTC)
		f.Set(reflect.ValueOf(t))
	} else {
		f.SetZero()
	}
}

func deserializeStringFromRedis(v string, f reflect.Value) {
	f.SetString(v)
}

func deserializeUIntegersPointersFromRedis(v string, f reflect.Value, size int) {
	if v != "" {
		asInt, _ := strconv.ParseUint(v, 10, 64)
		switch size {
		case 0:
			val := uint(asInt)
			f.Set(reflect.ValueOf(&val))
		case 8:
			val := uint8(asInt)
			f.Set(reflect.ValueOf(&val))
		case 16:
			val := uint16(asInt)
			f.Set(reflect.ValueOf(&val))
		case 32:
			val := uint32(asInt)
			f.Set(reflect.ValueOf(&val))
		case 64:
			f.Set(reflect.ValueOf(&asInt))
		}
		return
	}
	f.SetZero()
}

func deserializeIntegersPointersFromRedis(v string, f reflect.Value, size int) {
	if v != "" {
		asInt, _ := strconv.ParseInt(v, 10, 64)
		switch size {
		case 0:
			val := int(asInt)
			f.Set(reflect.ValueOf(&val))
		case 8:
			val := int8(asInt)
			f.Set(reflect.ValueOf(&val))
		case 16:
			val := int16(asInt)
			f.Set(reflect.ValueOf(&val))
		case 32:
			val := int32(asInt)
			f.Set(reflect.ValueOf(&val))
		case 64:
			f.Set(reflect.ValueOf(&asInt))
		}
		return
	}
	f.SetZero()
}

func deserializeBytesFromRedis(v string, f reflect.Value) {
	if v == "" {
		f.SetZero()
	} else {
		f.SetBytes([]byte(v))
	}
}

func deserializeSliceStringFromRedis(v string, f reflect.Value) {
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

func deserializeBoolPointersFromRedis(v string, f reflect.Value) {
	if v == cacheNilValue {
		f.SetZero()
	} else {
		b := v != zeroAsString
		f.Set(reflect.ValueOf(&b))
	}
}

func deserializeFloatPointersFromRedis(v string, f reflect.Value, size int) {
	if v != "" {
		asFloat, _ := strconv.ParseFloat(v, 64)
		if size == 32 {
			val := float32(asFloat)
			f.Set(reflect.ValueOf(&val))
			return
		}
		f.Set(reflect.ValueOf(&asFloat))
		return
	}
	f.SetZero()
}

func deserializeTimePointersFromRedis(v string, f reflect.Value) {
	if v != "" {
		t, _ := time.ParseInLocation(time.DateTime, v, time.UTC)
		f.Set(reflect.ValueOf(&t))
		return
	}
	f.SetZero()
}

func deserializeDatePointersFromRedis(v string, f reflect.Value) {
	if v != "" {
		t, _ := time.ParseInLocation(time.DateOnly, v, time.UTC)
		f.Set(reflect.ValueOf(&t))
		return
	}
	f.SetZero()
}

func deserializeStructFromDB(elem reflect.Value, index int, fields *tableFields, pointers []interface{}) int {
	for _, i := range fields.uIntegers {
		elem.Field(i).SetUint(*pointers[index].(*uint64))
		index++
	}
	for _, i := range fields.uIntegersArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			f.Index(j).SetUint(*pointers[index].(*uint64))
			index++
		}
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
	for _, i := range fields.referencesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullInt64)
			if v.Valid {
				arrayField := f.Index(j)
				val := reflect.New(arrayField.Type().Elem())
				reference := val.Interface().(referenceInterface)
				reference.SetID(uint64(v.Int64))
				arrayField.Set(val)
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(*pointers[index].(*int64))
		index++
	}
	for _, i := range fields.integersArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			f.Index(j).SetInt(*pointers[index].(*int64))
			index++
		}
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for _, i := range fields.booleansArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			f.Index(j).SetBool(*pointers[index].(*uint64) > 0)
			index++
		}
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(*pointers[index].(*float64))
		index++
	}
	for _, i := range fields.floatsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			f.Index(j).SetFloat(*pointers[index].(*float64))
			index++
		}
	}
	for _, i := range fields.times {
		v := *pointers[index].(*string)
		if v == zeroTimeAsString {
			elem.Field(i).SetZero()
		} else {
			t, _ := time.ParseInLocation(time.DateTime, v, time.UTC)
			elem.Field(i).Set(reflect.ValueOf(t))
		}
		index++
	}
	for _, i := range fields.timesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := *pointers[index].(*string)
			if v == zeroTimeAsString {
				f.Index(j).SetZero()
			} else {
				t, _ := time.ParseInLocation(time.DateTime, v, time.UTC)
				f.Index(j).Set(reflect.ValueOf(t))
			}
			index++
		}
	}
	for _, i := range fields.dates {
		v := *pointers[index].(*string)
		if v == zeroDateAsString {
			elem.Field(i).SetZero()
		} else {
			t, _ := time.ParseInLocation(time.DateOnly, v, time.UTC)
			elem.Field(i).Set(reflect.ValueOf(t))
		}
		index++
	}
	for _, i := range fields.datesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := *pointers[index].(*string)
			if v == zeroDateAsString {
				f.Index(j).SetZero()
			} else {
				t, _ := time.ParseInLocation(time.DateOnly, v, time.UTC)
				f.Index(j).Set(reflect.ValueOf(t))
			}
			index++
		}
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(pointers[index].(*sql.NullString).String)
		index++
	}
	for _, i := range fields.stringsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			f.Index(j).SetString(pointers[index].(*sql.NullString).String)
			index++
		}
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
	for _, i := range fields.uIntegersNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullInt64)
			if v.Valid {
				field := f.Index(j)
				val := reflect.New(field.Type().Elem())
				val.Elem().SetUint(uint64(v.Int64))
				field.Set(val)
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
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
	for _, i := range fields.integersNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullInt64)
			if v.Valid {
				field := f.Index(j)
				val := reflect.New(field.Type().Elem())
				val.Elem().SetInt(v.Int64)
				field.Set(val)
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
	}
	for _, i := range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		elem.Field(i).SetString(v.String)
		index++
	}
	for _, i := range fields.stringsEnumsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullString)
			f.Index(j).SetString(v.String)
			index++
		}
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
	for _, i := range fields.bytesArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullString)
			if v.Valid {
				f.Index(j).SetBytes([]byte(v.String))
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
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
	for _, i := range fields.sliceStringsSetsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullString)
			if v.Valid && v.String != "" {
				field := f.Index(j)
				values := strings.Split(v.String, ",")
				setValues := reflect.MakeSlice(field.Type(), len(values), len(values))
				for k, val := range strings.Split(v.String, ",") {
					setValues.Index(k).SetString(val)
				}
				field.Set(setValues)
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
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
	for _, i := range fields.booleansNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullBool)
			if v.Valid {
				field := f.Index(j)
				val := reflect.New(field.Type().Elem())
				val.Elem().SetBool(v.Bool)
				field.Set(val)
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
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
	for _, i := range fields.floatsNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullFloat64)
			if v.Valid {
				field := f.Index(j)
				val := reflect.New(field.Type().Elem())
				val.Elem().SetFloat(v.Float64)
				field.Set(val)
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
	}
	for _, i := range fields.timesNullable {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			if v.String == zeroTimeAsString {
				elem.Field(i).SetZero()
			} else {
				t, _ := time.ParseInLocation(time.DateTime, v.String, time.UTC)
				elem.Field(i).Set(reflect.ValueOf(&t))
			}
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.timesNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullString)
			if v.Valid {
				if v.String == zeroTimeAsString {
					f.Index(j).SetZero()
				} else {
					t, _ := time.ParseInLocation(time.DateTime, v.String, time.UTC)
					f.Index(j).Set(reflect.ValueOf(&t))
				}
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
	}
	for _, i := range fields.datesNullable {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			if v.String == zeroDateAsString {
				elem.Field(i).SetZero()
			} else {
				t, _ := time.ParseInLocation(time.DateOnly, v.String, time.UTC)
				elem.Field(i).Set(reflect.ValueOf(&t))
			}
		} else {
			elem.Field(i).SetZero()
		}
		index++
	}
	for _, i := range fields.datesNullableArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			v := pointers[index].(*sql.NullString)
			if v.Valid {
				if v.String == zeroDateAsString {
					f.Index(j).SetZero()
				} else {
					t, _ := time.ParseInLocation(time.DateOnly, v.String, time.UTC)
					f.Index(j).Set(reflect.ValueOf(&t))
				}
			} else {
				f.Index(j).SetZero()
			}
			index++
		}
	}
	for k, i := range fields.structs {
		index = deserializeStructFromDB(elem.Field(i), index, fields.structsFields[k], pointers)
	}
	for k, i := range fields.structsArray {
		f := elem.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			index = deserializeStructFromDB(f.Index(j), index, fields.structsFieldsArray[k], pointers)
		}
	}
	return index
}
