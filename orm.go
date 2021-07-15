package beeorm

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"

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
	serializer := engine.getSerializer()
	serializer.Reset(binary)
	v, _, _ := getFieldForStruct(fields, serializer, index, 0)
	return v
}

func getFieldForStruct(fields *tableFields, serializer *serializer, index, i int) (interface{}, bool, int) {
	for range fields.refs {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.uintegers {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.integers {
		v := serializer.GetInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.booleans {
		if i == index {
			return serializer.GetBool(), true, i
		}
		serializer.buffer.Next(1)
		i++
	}
	for range fields.floats {
		v := serializer.GetFloat()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.times {
		v := serializer.GetInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.dates {
		v := serializer.GetInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	if fields.fakeDelete > 0 {
		if i == index {
			return serializer.GetBool(), true, i
		}
		serializer.buffer.Next(1)
		i++
	}
	for range fields.strings {
		if i == index {
			return serializer.GetString(), true, i
		}
		if l := serializer.GetUInteger(); l > 0 {
			serializer.buffer.Next(int(l))
		}
		i++
	}
	for range fields.uintegersNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetUInteger(), true, i
		}
		serializer.GetUInteger()
		i++
	}
	for range fields.integersNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetInteger(), true, i
		}
		serializer.GetInteger()
		i++
	}
	for range fields.stringsEnums {
		v := serializer.GetUInteger()
		if i == index {
			return int(v), true, i
		}
		i++
	}
	for range fields.bytes {
		if i == index {
			return serializer.GetBytes(), true, i
		}
		if l := serializer.GetUInteger(); l > 0 {
			serializer.buffer.Next(int(l))
		}
		i++
	}
	for range fields.sliceStringsSets {
		l := int(serializer.GetUInteger())
		if i == index {
			val := make([]int, l)
			for k := 0; k < l; k++ {
				val[k] = int(serializer.GetUInteger())
			}
			return val, true, i
		}
		serializer.buffer.Next(l)
		i++
	}
	for range fields.booleansNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetBool(), true, i
		}
		serializer.GetBool()
		i++
	}
	for range fields.floatsNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetFloat(), true, i
		}
		serializer.GetFloat()
		i++
	}
	for range fields.timesNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetInteger(), true, i
		}
		serializer.GetInteger()
		i++
	}
	for range fields.datesNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetInteger(), true, i
		}
		serializer.GetInteger()
		i++
	}
	for range fields.jsons {
		if i == index {
			return serializer.GetBytes(), true, i
		}
		if l := serializer.GetUInteger(); l > 0 {
			serializer.buffer.Next(int(l))
		}
		i++
	}
	for range fields.refsMany {
		l := int(serializer.GetUInteger())
		if i == index {
			val := make([]uint64, l)
			for k := 0; k < l; k++ {
				val[k] = serializer.GetUInteger()
			}
			return val, true, i
		}
		serializer.buffer.Next(l)
		i++
	}
	for _, subFields := range fields.structsFields {
		v, has, j := getFieldForStruct(subFields, serializer, index, i)
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
		engine.getSerializer().Reset(orm.binary)
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
	if !orm.loaded {
		return true
	}
	_, is := orm.GetDirtyBind(engine)
	return is
}

func (orm *ORM) GetDirtyBind(engine *Engine) (bind Bind, has bool) {
	bind, _, _, has = orm.getDirtyBind(engine)
	return bind, has
}

func (orm *ORM) getDirtyBind(engine *Engine) (bind, current Bind, updateBind map[string]string, has bool) {
	if orm.fakeDelete {
		if orm.tableSchema.hasFakeDelete {
			orm.elem.FieldByName("FakeDelete").SetBool(true)
		} else {
			orm.delete = true
		}
	}
	id := orm.GetID()
	bind = make(Bind)
	if orm.inDB {
		if !orm.delete {
			updateBind = make(map[string]string)
		}
		if orm.delete || orm.tableSchema.hasLog || len(orm.tableSchema.cachedIndexesAll) > 0 {
			current = make(Bind)
		}
	}
	serializer := engine.getSerializer()
	serializer.Reset(orm.binary)
	orm.buildBind(id, serializer, bind, current, updateBind, orm.tableSchema, orm.tableSchema.fields, orm.elem, "", -1)
	has = orm.delete || len(bind) > 0
	return bind, current, updateBind, has
}

func (orm *ORM) serialize(serializer *serializer) {
	orm.serializeFields(serializer, orm.tableSchema.fields, orm.elem)
	orm.binary = serializer.Serialize()
	serializer.buffer.Reset()
}

func (orm *ORM) deserializeFromDB(engine *Engine, pointers []interface{}) {
	serializer := engine.getSerializer()
	serializer.buffer.Reset()
	deserializeStructFromDB(engine, serializer, 0, orm.tableSchema.fields, pointers)
	orm.binary = serializer.Serialize()
}

func deserializeStructFromDB(engine *Engine, serializer *serializer, index int, fields *tableFields, pointers []interface{}) int {
	for range fields.refs {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetUInteger(uint64(v.Int64))
		index++
	}
	for range fields.uintegers {
		serializer.SetUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.integers {
		serializer.SetInteger(*pointers[index].(*int64))
		index++
	}
	for range fields.booleans {
		serializer.SetBool(*pointers[index].(*bool))
		index++
	}
	for range fields.floats {
		serializer.SetFloat(*pointers[index].(*float64))
		index++
	}
	for range fields.times {
		unix := *pointers[index].(*int64)
		if unix != 0 {
			unix -= engine.registry.timeOffset
		}
		serializer.SetInteger(unix)
		index++
	}
	for range fields.dates {
		unix := *pointers[index].(*int64)
		if unix != 0 {
			unix -= engine.registry.timeOffset
		}
		serializer.SetInteger(unix)
		index++
	}
	if fields.fakeDelete > 0 {
		serializer.SetBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for range fields.strings {
		serializer.SetString(pointers[index].(*sql.NullString).String)
		index++
	}
	for range fields.uintegersNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInteger(v.Int64)
		}
		index++
	}
	k := 0
	for range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			serializer.SetUInteger(uint64(fields.enums[k].Index(v.String)))
		} else {
			serializer.SetUInteger(0)
		}
		index++
		k++
	}
	for range fields.bytes {
		serializer.SetBytes([]byte(pointers[index].(*sql.NullString).String))
		index++
	}
	k = 0
	for range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid && v.String != "" {
			values := strings.Split(v.String, ",")
			serializer.SetUInteger(uint64(len(values)))
			enum := fields.enums[k]
			for _, set := range values {
				serializer.SetUInteger(uint64(enum.Index(set)))
			}
		} else {
			serializer.SetUInteger(0)
		}
		k++
		index++
	}
	for range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetBool(v.Bool)
		}
		index++
	}
	for range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetFloat(v.Float64)
		}
		index++
	}
	for range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInteger(v.Int64 - engine.registry.timeOffset)
		}
		index++
	}
	for range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInteger(v.Int64 - engine.registry.timeOffset)
		}
		index++
	}
	for range fields.jsons {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			serializer.SetBytes([]byte(v.String))
		} else {
			serializer.SetBytes(nil)
		}
		index++
	}
	for range fields.refsMany {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			var slice []uint8
			_ = jsoniter.ConfigFastest.UnmarshalFromString(v.String, &slice)
			serializer.SetUInteger(uint64(len(slice)))
			for _, i := range slice {
				serializer.SetUInteger(uint64(i))
			}
		} else {
			serializer.SetUInteger(0)
		}
		index++
	}
	for _, subField := range fields.structsFields {
		index += deserializeStructFromDB(engine, serializer, index, subField, pointers)
	}
	return index
}

func (orm *ORM) serializeFields(serializer *serializer, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.refs {
		f := elem.Field(i)
		id := uint64(0)
		if !f.IsNil() {
			id = f.Elem().Field(1).Uint()
		}
		serializer.SetUInteger(id)
	}
	for _, i := range fields.uintegers {
		serializer.SetUInteger(elem.Field(i).Uint())
	}
	for _, i := range fields.integers {
		serializer.SetInteger(elem.Field(i).Int())
	}
	for _, i := range fields.booleans {
		serializer.SetBool(elem.Field(i).Bool())
	}
	for k, i := range fields.floats {
		f := elem.Field(i).Float()
		p := math.Pow10(fields.floatsPrecision[k])
		serializer.SetFloat(math.Round(f*p) / p)
	}
	for _, i := range fields.times {
		t := elem.Field(i).Interface().(time.Time)
		if t.IsZero() {
			serializer.SetInteger(0)
		} else {
			serializer.SetInteger(t.Unix())
		}
	}
	for _, i := range fields.dates {
		t := elem.Field(i).Interface().(time.Time)
		if t.IsZero() {
			serializer.SetInteger(0)
		} else {
			serializer.SetInteger(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix())
		}
	}
	if fields.fakeDelete > 0 {
		serializer.SetBool(elem.Field(fields.fakeDelete).Bool())
	}
	for _, i := range fields.strings {
		serializer.SetString(elem.Field(i).String())
	}
	for _, i := range fields.uintegersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInteger(f.Elem().Uint())
		}
	}
	for _, i := range fields.integersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInteger(f.Elem().Int())
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		val := elem.Field(i).String()
		if val == "" {
			serializer.SetUInteger(0)
		} else {
			serializer.SetUInteger(uint64(fields.enums[k].Index(val)))
		}
		k++
	}
	for _, i := range fields.bytes {
		serializer.SetBytes(elem.Field(i).Bytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		f := elem.Field(i)
		values := f.Interface().([]string)
		l := len(values)
		serializer.SetUInteger(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for _, val := range values {
				serializer.SetUInteger(uint64(set.Index(val)))
			}
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetBool(f.Elem().Bool())
		}
	}
	for k, i := range fields.floatsNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			val := f.Elem().Float()
			p := math.Pow10(fields.floatsNullablePrecision[k])
			serializer.SetFloat(math.Round(val*p) / p)
		}
	}
	for _, i := range fields.timesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInteger(f.Interface().(*time.Time).Unix())
		}
	}
	for _, i := range fields.datesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			t := f.Interface().(*time.Time)
			serializer.SetInteger(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix())
		}
	}
	for _, i := range fields.jsons {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBytes(nil)
		} else {
			encoded, _ := jsoniter.ConfigFastest.Marshal(f.Interface())
			serializer.SetBytes(encoded)
		}
	}
	for _, i := range fields.refsMany {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.SetUInteger(0)
		} else {
			l := e.Len()
			serializer.SetUInteger(uint64(l))
			for k := 0; k < l; k++ {
				serializer.SetUInteger(e.Index(k).Elem().Field(1).Uint())
			}
		}
	}
	for k, i := range fields.structs {
		orm.serializeFields(serializer, fields.structsFields[k], elem.Field(i))
	}
}

func (orm *ORM) deserialize(engine *Engine) {
	orm.deserializeFields(engine, orm.tableSchema.fields, orm.elem)
	orm.loaded = true
}

func (orm *ORM) deserializeFields(engine *Engine, fields *tableFields, elem reflect.Value) {
	serializer := engine.getSerializer()
	k := 0
	for _, i := range fields.refs {
		id := serializer.GetUInteger()
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
		elem.Field(i).SetUint(serializer.GetUInteger())
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(serializer.GetInteger())
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(serializer.GetBool())
	}
	for _, i := range fields.floats {
		elem.Field(i).SetFloat(serializer.GetFloat())
	}
	for _, i := range fields.times {
		f := elem.Field(i)
		unix := serializer.GetInteger()
		if unix == 0 {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix, 0)))
		}
	}
	for _, i := range fields.dates {
		f := elem.Field(i)
		unix := serializer.GetInteger()
		if unix == 0 {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.Set(reflect.ValueOf(time.Unix(unix, 0)))
		}
	}
	if fields.fakeDelete > 0 {
		elem.Field(fields.fakeDelete).SetBool(serializer.GetBool())
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(serializer.GetString())
	}
	for k, i := range fields.uintegersNullable {
		if serializer.GetBool() {
			v := serializer.GetUInteger()
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
		if serializer.GetBool() {
			v := serializer.GetInteger()
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
		index := serializer.GetUInteger()
		if index == 0 {
			elem.Field(i).SetString("")
		} else {
			elem.Field(i).SetString(fields.enums[k].GetFields()[index-1])
		}
		k++
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes(serializer.GetBytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		l := int(serializer.GetUInteger())
		f := elem.Field(i)
		if l == 0 {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		} else {
			enum := fields.enums[k]
			v := make([]string, l)
			for j := 0; j < l; j++ {
				v[j] = enum.GetFields()[serializer.GetUInteger()-1]
			}
			f.Set(reflect.ValueOf(v))
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		if serializer.GetBool() {
			v := serializer.GetBool()
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
		if serializer.GetBool() {
			v := serializer.GetFloat()
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
		if serializer.GetBool() {
			v := time.Unix(serializer.GetInteger(), 0)
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
		if serializer.GetBool() {
			v := time.Unix(serializer.GetInteger(), 0)
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
		bytes := serializer.GetBytes()
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
		l := int(serializer.GetUInteger())
		f := elem.Field(i)
		refType := fields.refsManyTypes[k]
		if l > 0 {
			slice := reflect.MakeSlice(reflect.SliceOf(refType), l, l)
			for j := 0; j < l; j++ {
				e := getTableSchema(engine.registry, fields.refsTypes[k]).NewEntity()
				o := e.getORM()
				o.idElem.SetUint(serializer.GetUInteger())
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

func (orm *ORM) buildBind(id uint64, serializer *serializer, bind, current Bind, updateBind map[string]string, tableSchema *tableSchema,
	fields *tableFields, value reflect.Value, prefix string, index int) int {
	hasUpdate := updateBind != nil
	hasCurrent := current != nil
	noPrefix := prefix == ""
	hasOld := orm.inDB
	for _, i := range fields.refs {
		index++
		f := value.Field(i)
		val := uint64(0)
		if !f.IsNil() {
			val = f.Elem().Field(1).Uint()
		}
		if hasOld {
			old := serializer.GetUInteger()
			if hasCurrent {
				if old == 0 {
					current[tableSchema.columnNames[index]] = nil
				} else {
					current[tableSchema.columnNames[index]] = old
				}
			}
			if old == val {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if val == 0 {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		}
	}
	for _, i := range fields.uintegers {
		index++
		val := value.Field(i).Uint()
		if i == 1 && noPrefix {
			serializer.GetUInteger()
			continue
		}
		if hasOld {
			old := serializer.GetUInteger()
			if hasCurrent {
				current[tableSchema.columnNames[index]] = old
			}
			if old == val {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(val, 10)
		}
	}
	for _, i := range fields.integers {
		index++
		val := value.Field(i).Int()
		if hasOld {
			old := serializer.GetInteger()
			if hasCurrent {
				current[tableSchema.columnNames[index]] = old
			}
			if old == val {
				continue
			}
		}

		name := tableSchema.columnNames[index]
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(val, 10)
		}
	}
	for _, i := range fields.booleans {
		index++
		val := value.Field(i).Bool()
		if hasOld {
			old := serializer.GetBool()
			if hasCurrent {
				current[tableSchema.columnNames[index]] = old
			}
			if old == val {
				continue
			}
		}

		name := tableSchema.columnNames[index]
		bind[name] = val
		if hasUpdate {
			if val {
				updateBind[name] = "1"
			} else {
				updateBind[name] = "0"
			}
		}
	}
	for k, i := range fields.floats {
		index++
		val := value.Field(i).Float()
		if hasOld {
			old := serializer.GetFloat()
			if hasCurrent {
				current[tableSchema.columnNames[index]] = old
			}
			if math.Abs(val-old) < (1 / float64(fields.floatsPrecision[k])) {
				continue
			}
		}

		name := tableSchema.columnNames[index]
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
		}
	}
	for _, i := range fields.times {
		index++
		f := value.Field(i)
		t := f.Interface().(time.Time)
		if hasOld {
			old := serializer.GetInteger()
			if hasCurrent {
				current[tableSchema.columnNames[index]] = time.Unix(old, 0).Format(timeFormat)
			}
			if (old == 0 && f.IsZero()) || (old == t.Unix()) {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		asString := t.Format(timeFormat)
		bind[name] = asString
		if hasUpdate {
			updateBind[name] = "'" + asString + "'"
		}
	}
	for _, i := range fields.dates {
		index++
		t := value.Field(i).Interface().(time.Time)
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		if hasOld {
			old := serializer.GetInteger()
			if hasCurrent {
				current[tableSchema.columnNames[index]] = time.Unix(old, 0).Format(dateformat)
			}
			if old == 0 && t.IsZero() || old == t.Unix() {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		asString := t.Format(dateformat)
		bind[name] = asString
		if hasUpdate {
			updateBind[name] = "'" + asString + "'"
		}
	}
	if fields.fakeDelete > 0 {
		index++
		val := value.Field(fields.fakeDelete).Bool()
		fakeID := uint64(0)
		if val {
			fakeID = id
		}
		if hasCurrent && hasOld {
			current[tableSchema.columnNames[index]] = fakeID
		}
		add := true
		if hasOld {
			old := serializer.GetBool()
			if hasCurrent {
				if old {
					current[tableSchema.columnNames[index]] = id
				} else {
					current[tableSchema.columnNames[index]] = uint64(0)
				}
			}
			if old == val {
				add = false
			}
		}
		if add {
			name := tableSchema.columnNames[index]
			bind[name] = fakeID
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(fakeID, 10)
			}
		}
	}
	for _, i := range fields.strings {
		index++
		val := value.Field(i).String()
		name := tableSchema.columnNames[index]
		if hasOld {
			old := serializer.GetString()
			if hasCurrent {
				if old == "" {
					attributes := tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						current[tableSchema.columnNames[index]] = ""
					} else {
						current[tableSchema.columnNames[index]] = nil
					}
				} else {
					current[tableSchema.columnNames[index]] = old
				}
			}
			if old == val {
				continue
			}
		}
		if val != "" {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = escapeSQLParam(val)
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = ""
				if hasUpdate {
					updateBind[name] = "''"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.uintegersNullable {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := uint64(0)
		if !isNil {
			val = f.Elem().Uint()
		}
		if hasOld {
			old := serializer.GetBool()
			if !old && hasCurrent {
				current[tableSchema.columnNames[index]] = nil
			}
			if old {
				oldVal := serializer.GetUInteger()
				if hasCurrent {
					current[tableSchema.columnNames[index]] = oldVal
				}
				if oldVal == val && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		}
	}
	for _, i := range fields.integersNullable {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := int64(0)
		if !isNil {
			val = f.Elem().Int()
		}
		if hasOld {
			old := serializer.GetBool()
			if !old && hasCurrent {
				current[tableSchema.columnNames[index]] = nil
			}
			if old {
				oldVal := serializer.GetInteger()
				if hasCurrent {
					current[tableSchema.columnNames[index]] = oldVal
				}
				if oldVal == val && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatInt(val, 10)
			}
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		index++
		val := value.Field(i).String()
		enum := fields.enums[k]
		name := tableSchema.columnNames[index]
		k++
		if hasOld {
			old := serializer.GetUInteger()
			if hasCurrent {
				if old == 0 {
					attributes := tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						current[name] = ""
					} else {
						current[name] = nil
					}
				} else {
					current[name] = enum.GetFields()[old-1]
				}
			}
			if old == uint64(enum.Index(val)) {
				continue
			}
		}
		if val != "" {
			if !enum.Has(val) {
				panic(errors.New("unknown enum value for " + name + " - " + val))
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = "'" + val + "'"
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = enum.GetDefault()
				if hasUpdate {
					updateBind[name] = "'" + enum.GetDefault() + "'"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.bytes {
		index++
		val := string(value.Field(i).Bytes())
		if hasOld {
			old := serializer.GetString()
			if hasCurrent {
				if old != "" {
					current[tableSchema.columnNames[index]] = val
				} else {
					current[tableSchema.columnNames[index]] = nil
				}
			}
			if old == val {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if val != "" {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = escapeSQLParam(val)
			}
		} else {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		}
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		index++
		val := value.Field(i).Interface().([]string)
		set := fields.sets[k]
		l := len(val)
		k++
		name := tableSchema.columnNames[index]
		if hasOld {
			old := int(serializer.GetUInteger())
			if hasCurrent {
				attributes := tableSchema.tags[name]
				required, hasRequired := attributes["required"]
				if hasRequired && required == "true" {
					current[name] = ""
				} else {
					current[name] = nil
				}
			}
			if l == old {
				if l == 0 {
					continue
				}
				oldValues := make([]int, l)
				if hasCurrent {
					current[name] = ""
				}
				for j := range val {
					oldValues[j] = int(serializer.GetUInteger())
					if hasCurrent {
						current[name] = current[name].(string) + "," + set.GetFields()[oldValues[j]-1]
					}
				}
				if hasCurrent {
					current[name] = current[name].(string)[1:]
				}
				valid := true
			MAIN:
				for _, v := range val {
					enumIndex := set.Index(v)
					if enumIndex == 0 {
						panic(errors.New("unknown set value for " + tableSchema.columnNames[index] + " - " + v))
					}
					for _, o := range oldValues {
						if o == enumIndex {
							continue MAIN
						}
					}
					valid = false
					break
				}
				if valid {
					continue
				}
			}
		}
		if l > 0 {
			valAsString := strings.Join(val, ",")
			bind[name] = valAsString
			if hasUpdate {
				updateBind[name] = "'" + valAsString + "'"
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = ""
				if hasUpdate {
					updateBind[name] = "''"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.booleansNullable {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := false
		if !isNil {
			val = f.Elem().Bool()
		}
		if hasOld {
			old := serializer.GetBool()
			if !old && hasCurrent {
				current[tableSchema.columnNames[index]] = nil
			}
			if old {
				oldVal := serializer.GetBool()
				if hasCurrent {
					current[tableSchema.columnNames[index]] = oldVal
				}
				if oldVal == val && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				if val {
					updateBind[name] = "1"
				} else {
					updateBind[name] = "0"
				}
			}
		}
	}
	for k, i := range fields.floatsNullable {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := float64(0)
		if !isNil {
			val = f.Elem().Float()
		}
		if hasOld {
			old := serializer.GetBool()
			if !old && hasCurrent {
				current[tableSchema.columnNames[index]] = nil
			}
			if old {
				v := serializer.GetFloat()
				if hasCurrent {
					current[tableSchema.columnNames[index]] = v
				}
				if !isNil && math.Abs(val-v) < (1/float64(fields.floatsNullablePrecision[k])) {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		}
	}
	for _, i := range fields.timesNullable {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val *time.Time
		if !isNil {
			val = f.Interface().(*time.Time)
		}
		if hasOld {
			old := serializer.GetBool()
			if !old && hasCurrent {
				current[tableSchema.columnNames[index]] = nil
			}
			if old {
				oldVal := serializer.GetInteger()
				if hasCurrent {
					current[tableSchema.columnNames[index]] = time.Unix(oldVal, 0).Format(timeFormat)
				}
				if oldVal == val.Unix() && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			asString := val.Format(timeFormat)
			bind[name] = asString
			if hasUpdate {
				updateBind[name] = "'" + asString + "'"
			}
		}
	}
	for _, i := range fields.datesNullable {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val time.Time
		if !isNil {
			val = *f.Interface().(*time.Time)
			val = time.Date(val.Year(), val.Month(), val.Day(), 0, 0, 0, 0, val.Location())
		}
		if hasOld {
			old := serializer.GetBool()
			if !old && hasCurrent {
				current[tableSchema.columnNames[index]] = nil
			}
			if old {
				oldVal := serializer.GetInteger()
				if hasCurrent {
					current[tableSchema.columnNames[index]] = time.Unix(oldVal, 0).Format(dateformat)
				}
				if oldVal == val.Unix() && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := tableSchema.columnNames[index]
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			asString := val.Format(dateformat)
			bind[name] = asString
			if hasUpdate {
				updateBind[name] = "'" + asString + "'"
			}
		}
	}
	for _, i := range fields.jsons {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val interface{}
		asString := ""
		encoded := false
		name := tableSchema.columnNames[index]
		if !isNil {
			val = f.Interface()
		}
		if hasOld {
			old := serializer.GetBytes()
			if len(old) == 0 {
				if hasCurrent {
					attributes := tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						current[tableSchema.columnNames[index]] = ""
					} else {
						current[tableSchema.columnNames[index]] = nil
					}
				}
				if isNil {
					continue
				}
			} else {
				oldValue := reflect.New(f.Type()).Elem().Interface()
				newValue := reflect.New(f.Type()).Elem().Interface()
				_ = jsoniter.ConfigFastest.Unmarshal(old, &oldValue)
				v, err := jsoniter.ConfigFastest.Marshal(val)
				checkError(err)
				_ = jsoniter.ConfigFastest.Unmarshal(v, &newValue)
				encoded = true
				asString = string(v)
				if hasCurrent {
					current[tableSchema.columnNames[index]] = string(old)
				}
				if cmp.Equal(oldValue, newValue) {
					continue
				}
			}
		}
		if !isNil {
			if !encoded {
				v, _ := jsoniter.ConfigFastest.Marshal(val)
				asString = string(v)
			}
			bind[name] = asString
			if hasUpdate {
				updateBind[name] = escapeSQLParam(asString)
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = ""
				if hasUpdate {
					updateBind[name] = "''"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.refsMany {
		index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val string
		name := tableSchema.columnNames[index]
		if !isNil {
			length := f.Len()
			if length > 0 {
				ids := make([]uint64, length)
				for j := 0; j < length; j++ {
					ids[j] = f.Index(j).Interface().(Entity).GetID()
				}
				encoded, _ := jsoniter.ConfigFastest.Marshal(ids)
				val = string(encoded)
			}
		}
		if hasOld {
			l := int(serializer.GetUInteger())
			if l == 0 {
				if hasCurrent {
					attributes := tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						current[tableSchema.columnNames[index]] = ""
					} else {
						current[tableSchema.columnNames[index]] = nil
					}
				}
				if val == "" {
					continue
				}
			} else if val != "" {
				old := "[" + strconv.FormatUint(serializer.GetUInteger(), 10)
				for j := 1; j < l; j++ {
					old += "," + strconv.FormatUint(serializer.GetUInteger(), 10)
				}
				old += "]"
				if hasCurrent {
					current[tableSchema.columnNames[index]] = old
				}
				if old == val {
					continue
				}
			}
		}
		if val != "" {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = "'" + val + "'"
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = ""
				if hasUpdate {
					updateBind[name] = ""
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for k, i := range fields.structs {
		index = orm.buildBind(id, serializer, bind, current, updateBind, tableSchema, fields.structsFields[k], value.Field(i), fields.fields[i].Name, index)
	}
	return index
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
