package beeorm

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"time"
)

const zeroDateSeconds = 31622400
const timeStampSeconds = 62167219200

type Bind map[string]interface{}

func (b Bind) Get(key string) interface{} {
	return b[key]
}

func (m *insertableEntity[E]) GetBind() Bind {
	bind := Bind{}
	if m.entity.GetID() > 0 {
		bind["ID"] = m.entity.GetID()
	}
	fillBindFromOneSource(m.c, bind, m.value.Elem(), GetEntitySchema[E](m.c).getFields(), "")
	return bind
}

func (e *editableEntity[E]) GetBind() (new, old Bind) {
	new = Bind{}
	old = Bind{}
	fillBindFromTwoSources(e.c, new, old, e.value, reflect.ValueOf(e.source), GetEntitySchema[E](e.c).getFields())
	return
}

func fillBindFromOneSource(c Context, bind Bind, source reflect.Value, fields *tableFields, prefix string) {
	for _, i := range fields.uintegers {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Uint()
	}
	for _, i := range fields.integers {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Int()
	}
	for _, i := range fields.booleans {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Bool()
	}
	for k, i := range fields.floats {
		v := strconv.FormatFloat(source.Field(i).Float(), 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		bind[prefix+fields.fields[i].Name] = v
	}
	for _, i := range fields.times {
		v := source.Field(i).Interface().(time.Time).UTC()
		bind[prefix+fields.fields[i].Name] = v.Format(time.DateTime)
	}
	for _, i := range fields.dates {
		v := source.Field(i).Interface().(time.Time).UTC()
		bind[prefix+fields.fields[i].Name] = v.Format(time.DateOnly)
	}
	for _, i := range fields.strings {
		bind[prefix+fields.fields[i].Name] = source.Field(i).String()
	}
	for _, i := range fields.uintegersNullable {
		f := source.Field(i)
		if !f.IsNil() {
			bind[prefix+fields.fields[i].Name] = f.Elem().Uint()
			continue
		}
		bind[fields.fields[i].Name] = nil
	}
	for _, i := range fields.integersNullable {
		f := source.Field(i)
		if !f.IsNil() {
			bind[prefix+fields.fields[i].Name] = f.Elem().Int()
			continue
		}
		bind[prefix+fields.fields[i].Name] = nil
	}
	for k, i := range fields.stringsEnums {
		val := source.Field(i).String()
		def := fields.enums[k]
		if val == "" {
			if def.required {
				panic(fmt.Errorf("enum %s cannot be empty", prefix+fields.fields[i].Name))
			} else {
				bind[prefix+fields.fields[i].Name] = nil
			}
			continue
		}
		if !slices.Contains(def.GetFields(), val) {
			panic(fmt.Errorf("invalid enum %s value: %s", prefix+fields.fields[i].Name, val))
		}
		bind[prefix+fields.fields[i].Name] = val
	}
	for _, i := range fields.bytes {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Bytes()
	}
	for k, i := range fields.sliceStringsSets {
		f := source.Field(i)
		def := fields.sets[k]
		if f.IsNil() || f.Len() == 0 {
			if def.required {
				panic(fmt.Errorf("set %s cannot be empty", prefix+fields.fields[i].Name))
			} else {
				bind[prefix+fields.fields[i].Name] = nil
			}
		} else {
			s := c.getStringBuilder()
			for j := 0; j < f.Len(); j++ {
				v := f.Index(j).String()
				if !slices.Contains(def.GetFields(), v) {
					panic(fmt.Errorf("invalid set %s value: %s", prefix+fields.fields[i].Name, v))
				}
				if j > 0 {
					s.WriteString(",")
				}
				s.WriteString(string(v))
			}
			bind[prefix+fields.fields[i].Name] = s.String()
		}
	}
	for _, i := range fields.booleansNullable {
		f := source.Field(i)
		if !f.IsNil() {
			bind[prefix+fields.fields[i].Name] = f.Elem().Bool()
			continue
		}
		bind[prefix+fields.fields[i].Name] = nil
	}
	for k, i := range fields.floatsNullable {
		f := source.Field(i)
		if !f.IsNil() {
			v := strconv.FormatFloat(f.Elem().Float(), 'f', fields.floatsNullablePrecision[k], fields.floatsNullableSize[k])
			bind[prefix+fields.fields[i].Name] = v
			continue
		}
		bind[prefix+fields.fields[i].Name] = nil
	}
	for _, i := range fields.timesNullable {
		f := source.Field(i)
		if !f.IsNil() {
			t := f.Elem().Interface().(time.Time)
			bind[prefix+fields.fields[i].Name] = t.Format(time.DateTime)
			continue
		}
		bind[prefix+fields.fields[i].Name] = nil
	}
	for _, i := range fields.datesNullable {
		f := source.Field(i)
		if !f.IsNil() {
			t := f.Elem().Interface().(time.Time)
			bind[prefix+fields.fields[i].Name] = t.Format(time.DateOnly)
			continue
		}
		bind[prefix+fields.fields[i].Name] = nil
	}
	for j, i := range fields.structs {
		sub := fields.structsFields[j]
		fillBindFromOneSource(c, bind, source.Field(i), sub, prefix+sub.prefix)
	}
}

func fillBindFromTwoSources(c Context, bind, oldBind Bind, source, before reflect.Value, fields *tableFields) {
	for _, i := range fields.uintegers {
		v1 := source.Field(i).Uint()
		v2 := before.Field(i).Uint()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.integers {
		v1 := source.Field(i).Int()
		v2 := before.Field(i).Int()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.booleans {
		v1 := source.Field(i).Bool()
		v2 := before.Field(i).Bool()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for k, i := range fields.floats {
		v1 := strconv.FormatFloat(source.Field(i).Float(), 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		v2 := strconv.FormatFloat(before.Field(i).Float(), 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.times {
		v1 := source.Field(i).Interface().(time.Time).UTC()
		v2 := before.Field(i).Interface().(time.Time).UTC()
		if v1.Unix() != v2.Unix() {
			bind[fields.fields[i].Name] = v1.Format(time.DateTime)
			oldBind[fields.fields[i].Name] = v2.Format(time.DateTime)
		}
	}
	for _, i := range fields.dates {
		v1 := source.Field(i).Interface().(time.Time).UTC()
		v1 = time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
		v2 := before.Field(i).Interface().(time.Time).UTC()
		v2 = time.Date(v2.Year(), v2.Month(), v2.Day(), 0, 0, 0, 0, time.UTC)
		if v1.Unix() != v2.Unix() {
			bind[fields.fields[i].Name] = v1.Format(time.DateOnly)
			oldBind[fields.fields[i].Name] = v2.Format(time.DateOnly)
		}
	}
	for _, i := range fields.strings {
		v1 := source.Field(i).String()
		v2 := before.Field(i).String()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.uintegersNullable {
		v1 := uint64(0)
		v2 := uint64(0)
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Uint()
		}
		if !v2IsNil {
			v2 = f2.Uint()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for _, i := range fields.integersNullable {
		v1 := int64(0)
		v2 := int64(0)
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Int()
		}
		if !v2IsNil {
			v2 = f2.Int()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for _, i := range fields.stringsEnums {
		v1 := source.Field(i).String()
		v2 := before.Field(i).String()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.bytes {
		v1 := source.Field(i).Bytes()
		v2 := before.Field(i).Bytes()
		if !bytes.Equal(v1, v2) {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.sliceStringsSets {
		var v1 []string
		var v2 []string
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Interface().([]string)
		}
		if !v2IsNil {
			v2 = f2.Interface().([]string)
		}
		if v1IsNil != v2IsNil || slices.Equal(v1, v2) {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for _, i := range fields.booleansNullable {
		v1 := false
		v2 := false
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Bool()
		}
		if !v2IsNil {
			v2 = f2.Bool()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for k, i := range fields.floatsNullable {
		v1 := ""
		v2 := ""
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = strconv.FormatFloat(f1.Float(), 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		}
		if !v2IsNil {
			v2 = strconv.FormatFloat(f2.Float(), 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for _, i := range fields.timesNullable {
		var v1 time.Time
		var v2 time.Time
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Elem().Interface().(time.Time).UTC()
		}
		if !v2IsNil {
			v2 = f2.Elem().Interface().(time.Time).UTC()
		}
		if v1IsNil != v2IsNil || v1.Unix() != v2.Unix() {
			bind[fields.fields[i].Name] = v1.Format(time.DateTime)
			oldBind[fields.fields[i].Name] = v2.Format(time.DateTime)
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for _, i := range fields.datesNullable {
		var v1 time.Time
		var v2 time.Time
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Elem().Interface().(time.Time).UTC()
			v1 = time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
		}
		if !v2IsNil {
			v2 = f2.Elem().Interface().(time.Time).UTC()
			v2 = time.Date(v2.Year(), v2.Month(), v2.Day(), 0, 0, 0, 0, time.UTC)
		}
		if v1IsNil != v2IsNil || v1.Unix() != v2.Unix() {
			bind[fields.fields[i].Name] = v1.Format(time.DateOnly)
			oldBind[fields.fields[i].Name] = v2.Format(time.DateOnly)
			if v1IsNil {
				bind[fields.fields[i].Name] = nil
			}
			if v2IsNil {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for j := range fields.structs {
		fillBindFromTwoSources(c, bind, oldBind, source, before, fields.structsFields[j])
	}
}
