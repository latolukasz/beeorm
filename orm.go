package beeorm

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/pkg/errors"
)

const TimeFormat = "2006-01-02 15:04:05"
const DateFormat = "2006-01-02"
const zeroDateSeconds = 31622400
const timeStampSeconds = 62167219200

var timeSupportedLayouts = []string{TimeFormat, DateFormat, time.RFC3339}

var disableCacheHashCheck bool

type Entity interface {
	getORM() *ORM
	GetID() uint64
	markToDelete()
	IsLoaded() bool
	SetOnDuplicateKeyUpdate(bind Bind)
	SetField(field string, value interface{}) error
	Clone() Entity
	SetMetaData(key, value string)
	GetMetaData() Meta
}

type ORM struct {
	binary               []byte
	entitySchema         *entitySchema
	onDuplicateKeyUpdate Bind
	meta                 Meta
	initialised          bool
	loaded               bool
	inDB                 bool
	delete               bool
	lazy                 bool
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
}

func DisableCacheHashCheck() {
	disableCacheHashCheck = true
}

func (orm *ORM) getORM() *ORM {
	return orm
}

func (orm *ORM) SetMetaData(key, value string) {
	if orm.meta == nil {
		if value != "" {
			orm.meta = Meta{key: value}
		}
	} else if value == "" {
		delete(orm.meta, key)
	} else {
		orm.meta[key] = value
	}
}

func (orm *ORM) GetID() uint64 {
	if orm.lazy {
		panic(errors.New("getting ID from lazy flushed entity not allowed"))
	}
	if !orm.initialised {
		panic(errors.New("getting ID from non initialised entity not allowed"))
	}
	return orm.idElem.Uint()
}

func (orm *ORM) GetMetaData() Meta {
	return orm.meta
}

func (orm *ORM) Clone() Entity {
	newEntity := orm.entitySchema.NewEntity()
	for i, field := range orm.entitySchema.fields.fields {
		if field.IsExported() {
			newEntity.getORM().elem.Field(i).Set(orm.getORM().elem.Field(i))
		} else {
			for k := 0; k < orm.getORM().elem.Field(i).Type().NumField(); k++ {
				newEntity.getORM().elem.Field(i).Field(k).Set(orm.getORM().elem.Field(i).Field(k))
			}
		}
	}
	newEntity.getORM().idElem.SetUint(0)
	return newEntity
}

func (orm *ORM) copyBinary() []byte {
	b := make([]byte, len(orm.binary))
	copy(b, orm.binary)
	return b
}

func (orm *ORM) markToDelete() {
	orm.delete = true
}

func (orm *ORM) IsLoaded() bool {
	return orm.loaded
}

func (orm *ORM) SetOnDuplicateKeyUpdate(bind Bind) {
	orm.onDuplicateKeyUpdate = bind
}

func (orm *ORM) buildDirtyBind(serializer *serializer, forceFillOld bool) (entitySQLFlushData *entitySQLFlush, has bool) {
	serializer.Reset(orm.binary)
	builder := newEntitySQLFlushBuilder(orm, forceFillOld)
	builder.fill(serializer, orm.entitySchema.fields, orm.elem, true)
	has = !orm.inDB || orm.delete || len(builder.Update) > 0
	return builder.entitySQLFlush, has
}

func (orm *ORM) serialize(serializer *serializer) {
	orm.serializeFields(serializer, orm.entitySchema.fields, orm.elem, true)
	orm.binary = serializer.Read()
}

func (orm *ORM) deserializeFromDB(serializer *serializer, pointers []interface{}) {
	orm.deserializeStructFromDB(serializer, 0, orm.entitySchema.fields, pointers, true)
	orm.binary = serializer.Read()
}

func (orm *ORM) deserializeStructFromDB(serializer *serializer, index int, fields *tableFields, pointers []interface{}, root bool) int {
	if root {
		serializer.SerializeUInteger(orm.entitySchema.structureHash)
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
	for range fields.jsons {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			serializer.SerializeBytes([]byte(v.String))
		} else {
			serializer.SerializeBytes(nil)
		}
		index++
	}
	for _, subField := range fields.structsFields {
		index = orm.deserializeStructFromDB(serializer, index, subField, pointers, false)
	}
	return index
}

func (orm *ORM) serializeFields(serialized *serializer, fields *tableFields, elem reflect.Value, root bool) {
	if root {
		serialized.SerializeUInteger(orm.entitySchema.structureHash)
	}
	for _, i := range fields.uintegers {
		v := elem.Field(i).Uint()
		serialized.SerializeUInteger(v)
	}
	for _, i := range fields.refs {
		f := elem.Field(i)
		id := uint64(0)
		if !f.IsNil() {
			id = f.Interface().(Entity).GetID()
		}
		serialized.SerializeUInteger(id)
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
	for _, i := range fields.jsons {
		f := elem.Field(i)
		if f.IsNil() {
			serialized.SerializeBytes(nil)
		} else {
			encoded, _ := jsoniter.ConfigFastest.Marshal(f.Interface())
			serialized.SerializeBytes(encoded)
		}
	}
	for k, i := range fields.structs {
		orm.serializeFields(serialized, fields.structsFields[k], elem.Field(i), false)
	}
}

func (orm *ORM) deserialize(serializer *serializer) {
	serializer.Reset(orm.binary)
	hash := serializer.DeserializeUInteger()
	if !disableCacheHashCheck && hash != orm.entitySchema.structureHash {
		panic(fmt.Errorf("%s entity cache data use wrong hash", orm.entitySchema.t.String()))
	}
	orm.deserializeFields(serializer, orm.entitySchema.fields, orm.elem)
	orm.loaded = true
}

func (orm *ORM) deserializeFields(serializer *serializer, fields *tableFields, elem reflect.Value) {
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
			e := getEntitySchema(orm.entitySchema.registry, fields.refsTypes[k]).NewEntity()
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
	for _, i := range fields.jsons {
		bytes := serializer.DeserializeBytes()
		f := elem.Field(i)
		if bytes != nil {
			t := f.Type()
			if t.Kind().String() == "map" {
				f.Set(reflect.MakeMap(t))
				v := f.Addr().Interface()
				_ = jsoniter.ConfigFastest.Unmarshal(bytes, v)
			} else {
				v := reflect.New(f.Type())
				_ = jsoniter.ConfigFastest.Unmarshal(bytes, v.Interface())
				f.Set(v.Elem())
			}
		} else if !f.IsNil() {
			f.Set(reflect.Zero(f.Type()))
		}
	}
	for k, i := range fields.structs {
		orm.deserializeFields(serializer, fields.structsFields[k], elem.Field(i))
	}
}

func (orm *ORM) SetField(field string, value interface{}) error {
	asString, isString := value.(string)
	if isString {
		asStringLower := strings.ToLower(asString)
		if asStringLower == "nil" || asStringLower == "null" {
			value = nil
		}
	}
	if !orm.elem.IsValid() {
		return errors.New("entity is not loaded")
	}
	f := orm.elem.FieldByName(field)
	if !f.IsValid() {
		return fmt.Errorf("field %s not found", field)
	}
	if !f.CanSet() {
		return fmt.Errorf("field %s is not public", field)
	}
	typeName := f.Type().String()
	switch typeName {
	case "uint",
		"uint8",
		"uint16",
		"uint32",
		"uint64":
		val := uint64(0)
		if value != nil {
			valid := !isString
			if !isString {
				switch value.(type) {
				case float32:
					val = uint64(value.(float32))
				case float64:
					val = uint64(value.(float64))
				default:
					valid = false
				}
			}
			if !valid && value != "" {
				parsed, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
				if err != nil {
					return fmt.Errorf("%s value %v not valid", field, value)
				}
				val = parsed
			}
		}
		f.SetUint(val)
	case "*uint",
		"*uint8",
		"*uint16",
		"*uint32",
		"*uint64":
		valueOf := reflect.ValueOf(value)
		if value != nil && !valueOf.IsZero() {
			val := uint64(0)
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", reflect.Indirect(valueOf).Interface()), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
			switch typeName {
			case "*uint":
				v := uint(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint8":
				v := uint8(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint16":
				v := uint16(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint32":
				v := uint32(val)
				f.Set(reflect.ValueOf(&v))
			default:
				f.Set(reflect.ValueOf(&val))
			}
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "int",
		"int8",
		"int16",
		"int32",
		"int64":
		val := int64(0)
		if value != nil {
			valid := !isString
			if !isString {
				switch value.(type) {
				case float32:
					asFloat := value.(float32)
					if asFloat < 0 {
						return fmt.Errorf("%s value %v not valid", field, value)
					}
					val = int64(asFloat)
				case float64:
					asFloat := value.(float64)
					if asFloat < 0 {
						return fmt.Errorf("%s value %v not valid", field, value)
					}
					val = int64(asFloat)
				default:
					valid = false
				}
			}
			if !valid && value != "" {
				parsed, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
				if err != nil {
					return fmt.Errorf("%s value %v not valid", field, value)
				}
				val = parsed
			}
		}
		f.SetInt(val)
	case "*int",
		"*int8",
		"*int16",
		"*int32",
		"*int64":
		valueOf := reflect.ValueOf(value)
		if value != nil && !valueOf.IsZero() {
			val := int64(0)
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", reflect.Indirect(valueOf).Interface()), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
			switch typeName {
			case "*int":
				v := int(val)
				f.Set(reflect.ValueOf(&v))
			case "*int8":
				v := int8(val)
				f.Set(reflect.ValueOf(&v))
			case "*int16":
				v := int16(val)
				f.Set(reflect.ValueOf(&v))
			case "*int32":
				v := int32(val)
				f.Set(reflect.ValueOf(&v))
			default:
				f.Set(reflect.ValueOf(&val))
			}
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "string":
		if value == nil {
			f.SetString("")
		} else {
			f.SetString(fmt.Sprintf("%v", value))
		}
	case "[]string":
		_, ok := value.([]string)
		if !ok {
			return fmt.Errorf("%s value %v not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "[]uint8":
		_, ok := value.([]uint8)
		if !ok {
			return fmt.Errorf("%s value %v not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "bool":
		val := false
		asString := strings.ToLower(fmt.Sprintf("%v", value))
		if asString == "true" || asString == "1" {
			val = true
		}
		f.SetBool(val)
	case "*bool":
		valueOf := reflect.ValueOf(value)
		if value == nil || valueOf.IsZero() {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := false
			asString := strings.ToLower(fmt.Sprintf("%v", reflect.Indirect(valueOf).Interface()))
			if asString == "true" || asString == "1" {
				val = true
			}
			f.Set(reflect.ValueOf(&val))
		}
	case "float32",
		"float64":
		val := float64(0)
		if value != nil {
			valueString := fmt.Sprintf("%v", value)
			if valueString != "" {
				valueString = strings.ReplaceAll(valueString, ",", ".")
				parsed, err := strconv.ParseFloat(valueString, 64)
				if err != nil {
					return fmt.Errorf("%s value %v is not valid", field, value)
				}
				val = parsed
			}
		}
		f.SetFloat(val)
	case "*float32",
		"*float64":
		valueOf := reflect.ValueOf(value)
		if value == nil || valueOf.IsZero() {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := float64(0)
			valueString := fmt.Sprintf("%v", reflect.Indirect(valueOf).Interface())
			if valueString != "" {
				valueString = strings.ReplaceAll(valueString, ",", ".")
				parsed, err := strconv.ParseFloat(valueString, 64)
				if err != nil {
					return fmt.Errorf("%s value %v is not valid", field, value)
				}
				val = parsed
			}
			f.Set(reflect.ValueOf(&val))
		}
	case "*time.Time":
		valueOf := reflect.ValueOf(value)
		if value == nil || valueOf.IsZero() {
			f.Set(reflect.Zero(f.Type()))
		} else {
			if isString {
				for _, layout := range timeSupportedLayouts {
					asTime, err := time.Parse(layout, asString)
					if err == nil {
						f.Set(reflect.ValueOf(&asTime))
						return nil
					}
				}
				return fmt.Errorf("%s value %v is not valid", field, asString)
			}
			_, ok := value.(*time.Time)
			if !ok {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			f.Set(reflect.ValueOf(value))
		}
	case "time.Time":
		if isString {
			for _, layout := range timeSupportedLayouts {
				asTime, err := time.Parse(layout, asString)
				if err == nil {
					f.Set(reflect.ValueOf(asTime))
					return nil
				}
			}
			return fmt.Errorf("%s value %v is not valid", field, asString)
		}
		_, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("%s value %v is not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	default:
		k := f.Type().Kind().String()
		if k == "struct" || k == "slice" {
			f.Set(reflect.ValueOf(value))
		} else if k == "ptr" {
			modelType := reflect.TypeOf((*Entity)(nil)).Elem()
			if f.Type().Implements(modelType) {
				if value == nil || (isString && (value == "" || value == "0")) {
					f.Set(reflect.Zero(f.Type()))
				} else {
					asEntity, ok := value.(Entity)
					if ok {
						f.Set(reflect.ValueOf(asEntity))
					} else {
						id, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
						if err != nil {
							return fmt.Errorf("%s value %v is not valid", field, value)
						}
						if id == 0 {
							f.Set(reflect.Zero(f.Type()))
						} else {
							newRef := orm.entitySchema.registry.GetEntitySchema(f.Type().Elem().String()).NewEntity()
							newRef.getORM().idElem.SetUint(id)
							f.Set(reflect.ValueOf(newRef))
						}
					}
				}
			} else {
				return fmt.Errorf("field %s is not supported", field)
			}
		} else {
			return fmt.Errorf("field %s is not supported", field)
		}
	}
	return nil
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
