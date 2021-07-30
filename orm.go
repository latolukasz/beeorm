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

const timeFormat = "2006-01-02 15:04:05"
const dateformat = "2006-01-02"

type Entity interface {
	getORM() *ORM
	GetID() uint64
	markToDelete()
	forceMarkToDelete()
	IsLoaded() bool
	IsLazy() bool
	Fill(engine *Engine)
	IsDirty(engine *Engine) bool
	GetDirtyBind(engine *Engine) (bind Bind, has bool)
	SetOnDuplicateKeyUpdate(bind Bind)
	SetEntityLogMeta(key string, value interface{})
	SetField(field string, value interface{}) error
	GetFieldLazy(engine *Engine, field string) interface{}
}

type ORM struct {
	binary               []byte
	tableSchema          *tableSchema
	onDuplicateKeyUpdate map[string]interface{}
	initialised          bool
	loaded               bool
	lazy                 bool
	inDB                 bool
	delete               bool
	fakeDelete           bool
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
	logMeta              map[string]interface{}
}

func (orm *ORM) getORM() *ORM {
	return orm
}

func (orm *ORM) GetID() uint64 {
	if !orm.idElem.IsValid() {
		return 0
	}
	return orm.idElem.Uint()
}

func (orm *ORM) GetFieldLazy(engine *Engine, field string) interface{} {
	if !orm.lazy {
		panic(fmt.Errorf("entity is not lazy"))
	}
	return getFieldByName(engine, orm.tableSchema, orm.binary, field)
}

func (orm *ORM) copyBinary() []byte {
	b := make([]byte, len(orm.binary))
	copy(b, orm.binary)
	return b
}

func getFieldByName(engine *Engine, tableSchema *tableSchema, binary []byte, field string) interface{} {
	index, has := tableSchema.columnMapping[field]
	if !has {
		panic(fmt.Errorf("uknown field " + field))
	}
	return getField(engine, tableSchema, binary, index)
}

func getField(engine *Engine, tableSchema *tableSchema, binary []byte, index int) interface{} {
	fields := tableSchema.fields
	engine.bufferInit(binary)
	v, _, _ := getFieldForStruct(fields, engine, index, 0)
	return v
}

func getFieldForStruct(fields *tableFields, engine *Engine, index, i int) (interface{}, bool, int) {
	for range fields.refs {
		v := engine.deserializeUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.uintegers {
		v := engine.deserializeUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.integers {
		v := engine.deserializeInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.booleans {
		if i == index {
			return engine.deserializeBool(), true, i
		}
		engine.buffer.Next(1)
		i++
	}
	for range fields.floats {
		v := engine.deserializeFloat()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.times {
		v := engine.deserializeInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.dates {
		v := engine.deserializeInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	if fields.fakeDelete > 0 {
		if i == index {
			return engine.deserializeBool(), true, i
		}
		engine.buffer.Next(1)
		i++
	}
	for range fields.strings {
		if i == index {
			return engine.deserializeString(), true, i
		}
		if l := engine.deserializeUInteger(); l > 0 {
			engine.buffer.Next(int(l))
		}
		i++
	}
	for range fields.uintegersNullable {
		isNil := engine.deserializeBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return engine.deserializeUInteger(), true, i
		}
		engine.deserializeUInteger()
		i++
	}
	for range fields.integersNullable {
		isNil := engine.deserializeBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return engine.deserializeInteger(), true, i
		}
		engine.deserializeInteger()
		i++
	}
	for range fields.stringsEnums {
		v := engine.deserializeUInteger()
		if i == index {
			return int(v), true, i
		}
		i++
	}
	for range fields.bytes {
		if i == index {
			return engine.deserializeBytes(), true, i
		}
		if l := engine.deserializeUInteger(); l > 0 {
			engine.buffer.Next(int(l))
		}
		i++
	}
	for range fields.sliceStringsSets {
		l := int(engine.deserializeUInteger())
		if i == index {
			val := make([]int, l)
			for k := 0; k < l; k++ {
				val[k] = int(engine.deserializeUInteger())
			}
			return val, true, i
		}
		engine.buffer.Next(l)
		i++
	}
	for range fields.booleansNullable {
		isNil := engine.deserializeBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return engine.deserializeBool(), true, i
		}
		engine.deserializeBool()
		i++
	}
	for range fields.floatsNullable {
		isNil := engine.deserializeBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return engine.deserializeFloat(), true, i
		}
		engine.deserializeFloat()
		i++
	}
	for range fields.timesNullable {
		isNil := engine.deserializeBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return engine.deserializeInteger(), true, i
		}
		engine.deserializeInteger()
		i++
	}
	for range fields.datesNullable {
		isNil := engine.deserializeBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return engine.deserializeInteger(), true, i
		}
		engine.deserializeInteger()
		i++
	}
	for range fields.jsons {
		if i == index {
			return engine.deserializeBytes(), true, i
		}
		if l := engine.deserializeUInteger(); l > 0 {
			engine.buffer.Next(int(l))
		}
		i++
	}
	for range fields.refsMany {
		l := int(engine.deserializeUInteger())
		if i == index {
			val := make([]uint64, l)
			for k := 0; k < l; k++ {
				val[k] = engine.deserializeUInteger()
			}
			return val, true, i
		}
		engine.buffer.Next(l)
		i++
	}
	for _, subFields := range fields.structsFields {
		v, has, j := getFieldForStruct(subFields, engine, index, i)
		if has {
			return v, true, j
		}
		i = j
	}
	return nil, false, 0
}

func (orm *ORM) markToDelete() {
	orm.fakeDelete = true
}

func (orm *ORM) forceMarkToDelete() {
	orm.delete = true
}

func (orm *ORM) IsLoaded() bool {
	return orm.loaded
}

func (orm *ORM) IsLazy() bool {
	return orm.lazy
}

func (orm *ORM) Fill(engine *Engine) {
	if orm.lazy && orm.loaded {
		orm.deserialize(engine)
		orm.lazy = false
	}
}

func (orm *ORM) SetOnDuplicateKeyUpdate(bind Bind) {
	orm.onDuplicateKeyUpdate = bind
}

func (orm *ORM) SetEntityLogMeta(key string, value interface{}) {
	if orm.logMeta == nil {
		orm.logMeta = make(map[string]interface{})
	}
	orm.logMeta[key] = value
}

func (orm *ORM) IsDirty(engine *Engine) bool {
	if !orm.inDB {
		return true
	}
	_, is := orm.GetDirtyBind(engine)
	return is
}

func (orm *ORM) GetDirtyBind(engine *Engine) (bind Bind, has bool) {
	has = orm.buildDirtyBind(engine)
	return engine.bindBuilder.bind, has
}

func (orm *ORM) buildDirtyBind(engine *Engine) (has bool) {
	if orm.fakeDelete {
		if orm.tableSchema.hasFakeDelete {
			orm.elem.FieldByName("FakeDelete").SetBool(true)
		} else {
			orm.delete = true
		}
	}
	id := orm.GetID()
	engine.bufferInit(orm.binary)
	engine.initBindBuilder(id, orm).build(orm.tableSchema.fields, orm.elem, true)
	has = !orm.inDB || orm.delete || len(engine.bindBuilder.bind) > 0
	return has
}

func (orm *ORM) serialize(engine *Engine) {
	engine.bufferInit(nil)
	orm.serializeFields(engine, orm.tableSchema.fields, orm.elem)
	orm.binary = engine.bufferRead()
}

func (orm *ORM) deserializeFromDB(engine *Engine, pointers []interface{}) {
	engine.bufferInit(nil)
	deserializeStructFromDB(engine, 0, orm.tableSchema.fields, pointers)
	orm.binary = engine.bufferRead()
}

func deserializeStructFromDB(engine *Engine, index int, fields *tableFields, pointers []interface{}) int {
	for range fields.refs {
		v := pointers[index].(*sql.NullInt64)
		engine.serializeUInteger(uint64(v.Int64))
		index++
	}
	for range fields.uintegers {
		engine.serializeUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.integers {
		engine.serializeInteger(*pointers[index].(*int64))
		index++
	}
	for range fields.booleans {
		engine.serializeBool(*pointers[index].(*bool))
		index++
	}
	for range fields.floats {
		engine.serializeFloat(*pointers[index].(*float64))
		index++
	}
	for range fields.times {
		unix := *pointers[index].(*int64)
		if unix != 0 {
			unix -= engine.registry.timeOffset
		}
		engine.serializeInteger(unix)
		index++
	}
	for range fields.dates {
		unix := *pointers[index].(*int64)
		if unix != 0 {
			unix -= engine.registry.timeOffset
		}
		engine.serializeInteger(unix)
		index++
	}
	if fields.fakeDelete > 0 {
		engine.serializeBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for range fields.strings {
		engine.serializeString(pointers[index].(*sql.NullString).String)
		index++
	}
	for range fields.uintegersNullable {
		v := pointers[index].(*sql.NullInt64)
		engine.serializeBool(v.Valid)
		if v.Valid {
			engine.serializeUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		engine.serializeBool(v.Valid)
		if v.Valid {
			engine.serializeInteger(v.Int64)
		}
		index++
	}
	k := 0
	for range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			engine.serializeUInteger(uint64(fields.enums[k].Index(v.String)))
		} else {
			engine.serializeUInteger(0)
		}
		index++
		k++
	}
	for range fields.bytes {
		engine.serializeBytes([]byte(pointers[index].(*sql.NullString).String))
		index++
	}
	k = 0
	for range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid && v.String != "" {
			values := strings.Split(v.String, ",")
			engine.serializeUInteger(uint64(len(values)))
			enum := fields.enums[k]
			for _, set := range values {
				engine.serializeUInteger(uint64(enum.Index(set)))
			}
		} else {
			engine.serializeUInteger(0)
		}
		k++
		index++
	}
	for range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		engine.serializeBool(v.Valid)
		if v.Valid {
			engine.serializeBool(v.Bool)
		}
		index++
	}
	for range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		engine.serializeBool(v.Valid)
		if v.Valid {
			engine.serializeFloat(v.Float64)
		}
		index++
	}
	for range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		engine.serializeBool(v.Valid)
		if v.Valid {
			engine.serializeInteger(v.Int64 - engine.registry.timeOffset)
		}
		index++
	}
	for range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		engine.serializeBool(v.Valid)
		if v.Valid {
			engine.serializeInteger(v.Int64 - engine.registry.timeOffset)
		}
		index++
	}
	for range fields.jsons {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			engine.serializeBytes([]byte(v.String))
		} else {
			engine.serializeBytes(nil)
		}
		index++
	}
	for range fields.refsMany {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			var slice []uint8
			_ = jsoniter.ConfigFastest.UnmarshalFromString(v.String, &slice)
			engine.serializeUInteger(uint64(len(slice)))
			for _, i := range slice {
				engine.serializeUInteger(uint64(i))
			}
		} else {
			engine.serializeUInteger(0)
		}
		index++
	}
	for _, subField := range fields.structsFields {
		index = deserializeStructFromDB(engine, index, subField, pointers)
	}
	return index
}

func (orm *ORM) serializeFields(engine *Engine, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.refs {
		f := elem.Field(i)
		id := uint64(0)
		if !f.IsNil() {
			id = f.Elem().Field(1).Uint()
		}
		engine.serializeUInteger(id)
	}
	for _, i := range fields.uintegers {
		engine.serializeUInteger(elem.Field(i).Uint())
	}
	for _, i := range fields.integers {
		engine.serializeInteger(elem.Field(i).Int())
	}
	for _, i := range fields.booleans {
		engine.serializeBool(elem.Field(i).Bool())
	}
	for k, i := range fields.floats {
		f := elem.Field(i).Float()
		p := math.Pow10(fields.floatsPrecision[k])
		engine.serializeFloat(math.Round(f*p) / p)
	}
	for _, i := range fields.times {
		t := elem.Field(i).Interface().(time.Time)
		if t.IsZero() {
			engine.serializeInteger(0)
		} else {
			engine.serializeInteger(t.Unix())
		}
	}
	for _, i := range fields.dates {
		t := elem.Field(i).Interface().(time.Time)
		if t.IsZero() {
			engine.serializeInteger(0)
		} else {
			engine.serializeInteger(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix())
		}
	}
	if fields.fakeDelete > 0 {
		engine.serializeBool(elem.Field(fields.fakeDelete).Bool())
	}
	for _, i := range fields.strings {
		engine.serializeString(elem.Field(i).String())
	}
	for _, i := range fields.uintegersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBool(false)
		} else {
			engine.serializeBool(true)
			engine.serializeUInteger(f.Elem().Uint())
		}
	}
	for _, i := range fields.integersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBool(false)
		} else {
			engine.serializeBool(true)
			engine.serializeInteger(f.Elem().Int())
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		val := elem.Field(i).String()
		if val == "" {
			engine.serializeUInteger(0)
		} else {
			engine.serializeUInteger(uint64(fields.enums[k].Index(val)))
		}
		k++
	}
	for _, i := range fields.bytes {
		engine.serializeBytes(elem.Field(i).Bytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		f := elem.Field(i)
		values := f.Interface().([]string)
		l := len(values)
		engine.serializeUInteger(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for _, val := range values {
				engine.serializeUInteger(uint64(set.Index(val)))
			}
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBool(false)
		} else {
			engine.serializeBool(true)
			engine.serializeBool(f.Elem().Bool())
		}
	}
	for k, i := range fields.floatsNullable {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBool(false)
		} else {
			engine.serializeBool(true)
			val := f.Elem().Float()
			p := math.Pow10(fields.floatsNullablePrecision[k])
			engine.serializeFloat(math.Round(val*p) / p)
		}
	}
	for _, i := range fields.timesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBool(false)
		} else {
			engine.serializeBool(true)
			engine.serializeInteger(f.Interface().(*time.Time).Unix())
		}
	}
	for _, i := range fields.datesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBool(false)
		} else {
			engine.serializeBool(true)
			t := f.Interface().(*time.Time)
			engine.serializeInteger(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix())
		}
	}
	for _, i := range fields.jsons {
		f := elem.Field(i)
		if f.IsNil() {
			engine.serializeBytes(nil)
		} else {
			encoded, _ := jsoniter.ConfigFastest.Marshal(f.Interface())
			engine.serializeBytes(encoded)
		}
	}
	for _, i := range fields.refsMany {
		e := elem.Field(i)
		if e.IsNil() {
			engine.serializeUInteger(0)
		} else {
			l := e.Len()
			engine.serializeUInteger(uint64(l))
			for k := 0; k < l; k++ {
				engine.serializeUInteger(e.Index(k).Elem().Field(1).Uint())
			}
		}
	}
	for k, i := range fields.structs {
		orm.serializeFields(engine, fields.structsFields[k], elem.Field(i))
	}
}

func (orm *ORM) deserialize(engine *Engine) {
	engine.bufferInit(orm.binary)
	orm.deserializeFields(engine, orm.tableSchema.fields, orm.elem)
	orm.loaded = true
}

func (orm *ORM) deserializeFields(engine *Engine, fields *tableFields, elem reflect.Value) {
	k := 0
	for _, i := range fields.refs {
		id := engine.deserializeUInteger()
		f := elem.Field(i)
		isNil := f.IsNil()
		if id > 0 {
			if isNil {
				e := getTableSchema(engine.registry, fields.refsTypes[k]).NewEntity()
				o := e.getORM()
				o.idElem.SetUint(id)
				o.inDB = true
				f.Set(o.value)
			}
		} else if !isNil {
			elem.Field(i).Set(reflect.Zero(reflect.PtrTo(fields.refsTypes[k])))
		}
		k++
	}
	for _, i := range fields.uintegers {
		elem.Field(i).SetUint(engine.deserializeUInteger())
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(engine.deserializeInteger())
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(engine.deserializeBool())
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(engine.deserializeFloat())
	}
	for _, i := range fields.times {
		f := elem.Field(i)
		unix := engine.deserializeInteger()
		if unix == 0 {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix, 0)))
		}
	}
	for _, i := range fields.dates {
		f := elem.Field(i)
		unix := engine.deserializeInteger()
		if unix == 0 {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix, 0)))
		}
	}
	if fields.fakeDelete > 0 {
		elem.Field(fields.fakeDelete).SetBool(engine.deserializeBool())
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(engine.deserializeString())
	}
	for k, i := range fields.uintegersNullable {
		if engine.deserializeBool() {
			v := engine.deserializeUInteger()
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
		if engine.deserializeBool() {
			v := engine.deserializeInteger()
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
	k = 0
	for _, i := range fields.stringsEnums {
		index := engine.deserializeUInteger()
		if index == 0 {
			elem.Field(i).SetString("")
		} else {
			elem.Field(i).SetString(fields.enums[k].GetFields()[index-1])
		}
		k++
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes(engine.deserializeBytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		l := int(engine.deserializeUInteger())
		f := elem.Field(i)
		if l == 0 {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		} else {
			enum := fields.enums[k]
			v := make([]string, l)
			for j := 0; j < l; j++ {
				v[j] = enum.GetFields()[engine.deserializeUInteger()-1]
			}
			f.Set(reflect.ValueOf(v))
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		if engine.deserializeBool() {
			v := engine.deserializeBool()
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *bool
			f.Set(reflect.ValueOf(&v))
		}
	}
	for k, i := range fields.floatsNullable {
		if engine.deserializeBool() {
			v := engine.deserializeFloat()
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
		if engine.deserializeBool() {
			v := time.Unix(engine.deserializeInteger(), 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *time.Time
			f.Set(reflect.ValueOf(v))
		}
	}
	for _, i := range fields.datesNullable {
		if engine.deserializeBool() {
			v := time.Unix(engine.deserializeInteger(), 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
			continue
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *time.Time
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.jsons {
		bytes := engine.deserializeBytes()
		f := elem.Field(i)
		if bytes != nil {
			v := reflect.New(f.Type()).Interface()
			_ = jsoniter.ConfigFastest.Unmarshal(bytes, v)
			f.Set(reflect.ValueOf(v).Elem())
		} else {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		}
	}
	k = 0
	for _, i := range fields.refsMany {
		l := int(engine.deserializeUInteger())
		f := elem.Field(i)
		refType := fields.refsManyTypes[k]
		if l > 0 {
			slice := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(refType)), l, l)
			for j := 0; j < l; j++ {
				e := getTableSchema(engine.registry, fields.refsManyTypes[k]).NewEntity()
				o := e.getORM()
				o.idElem.SetUint(engine.deserializeUInteger())
				o.inDB = true
				slice.Index(j).Set(o.value)
			}
			f.Set(slice)
		} else {
			if !f.IsNil() {
				f.Set(reflect.Zero(reflect.SliceOf(refType)))
			}
		}
		k++
	}
	for k, i := range fields.structs {
		orm.deserializeFields(engine, fields.structsFields[k], elem.Field(i))
	}
}

func (orm *ORM) SetField(field string, value interface{}) error {
	asString, isString := value.(string)
	if isString {
		asString = strings.ToLower(asString)
		if asString == "nil" || asString == "null" {
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
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
		}
		f.SetUint(val)
	case "*uint",
		"*uint8",
		"*uint16",
		"*uint32",
		"*uint64":
		if value != nil {
			val := uint64(0)
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()), 10, 64)
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
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
		}
		f.SetInt(val)
	case "*int",
		"*int8",
		"*int16",
		"*int32",
		"*int64":
		if value != nil {
			val := int64(0)
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()), 10, 64)
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
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := false
			asString := strings.ToLower(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()))
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
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			val = parsed
		}
		f.SetFloat(val)
	case "*float32",
		"*float64":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := float64(0)
			valueString := fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface())
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			val = parsed
			f.Set(reflect.ValueOf(&val))
		}
	case "*time.Time":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			_, ok := value.(*time.Time)
			if !ok {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			f.Set(reflect.ValueOf(value))
		}
	case "time.Time":
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
							val := reflect.New(f.Type().Elem())
							val.Elem().FieldByName("ID").SetUint(id)
							f.Set(val)
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
