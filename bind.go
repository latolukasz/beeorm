package beeorm

import (
	"bytes"
	"strings"
	"time"
)

type Bind map[string]interface{}

func (b Bind) Get(key string) interface{} {
	return b[key]
}

func (m *insertableEntity[E]) GetBind() Bind {
	s := m.c.getSerializer()
	o := m.entity.getORM()
	o.serialize(s)
	bind := Bind{}
	fillBindFromOneSource(m.c, bind, s, GetEntitySchema[E](m.c).getFields(), true)
	return bind
}

func (e *editableEntity[E]) GetBind() (new, old Bind) {
	s := e.c.getSerializer()
	schema := GetEntitySchema[E](e.c)
	if e.delete {
		o := e.source.getORM()
		o.serialize(s)
		old = Bind{}
		fillBindFromOneSource(e.c, old, s, schema.getFields(), true)
		return nil, old
	}
	o := e.entity.getORM()
	o.serialize(s)
	s2 := e.c.getSerializer2()
	o2 := e.source.getORM()
	o2.serialize(s2)
	new = Bind{}
	old = Bind{}
	fillBindFromTwoSources(e.c, new, old, s, s2, schema.getFields(), true)
	return
}

func fillBindFromOneSource(c Context, bind Bind, source *serializer, fields *tableFields, root bool) {
	if root {
		source.DeserializeUInteger()
	}
	for _, i := range fields.uintegers {
		bind[fields.fields[i].Name] = source.DeserializeUInteger()
	}
	for _, i := range fields.refs {
		v := source.DeserializeUInteger()
		if v > 0 {
			bind[fields.fields[i].Name] = v
		} else {
			bind[fields.fields[i].Name] = nil
		}
	}
	for _, i := range fields.integers {
		bind[fields.fields[i].Name] = source.DeserializeInteger()
	}
	for _, i := range fields.booleans {
		bind[fields.fields[i].Name] = source.DeserializeBool()
	}
	for _, i := range fields.floats {
		bind[fields.fields[i].Name] = source.DeserializeFloat()
	}
	for _, i := range fields.times {
		v := source.DeserializeInteger()
		if v == zeroDateSeconds {
			bind[fields.fields[i].Name] = "0000-00-00 00:00:00"
		} else {
			bind[fields.fields[i].Name] = time.Unix(v-timeStampSeconds, 0).Format(time.DateTime)
		}
	}
	for _, i := range fields.dates {
		v := source.DeserializeInteger()
		if v == zeroDateSeconds {
			bind[fields.fields[i].Name] = "0000-00-00"
		} else {
			bind[fields.fields[i].Name] = time.Unix(v-timeStampSeconds, 0).Format(time.DateOnly)
		}
	}
	for _, i := range fields.strings {
		bind[fields.fields[i].Name] = source.DeserializeString()
	}
	for _, i := range fields.uintegersNullable {
		if source.DeserializeBool() {
			bind[fields.fields[i].Name] = source.DeserializeUInteger()
			continue
		}
		bind[fields.fields[i].Name] = nil
	}
	for _, i := range fields.integersNullable {
		if source.DeserializeBool() {
			bind[fields.fields[i].Name] = source.DeserializeInteger()
		}
		bind[fields.fields[i].Name] = nil
	}
	for z, i := range fields.stringsEnums {
		v := source.DeserializeUInteger()
		if v == 0 {
			bind[fields.fields[i].Name] = ""
		} else {
			bind[fields.fields[i].Name] = fields.enums[z].GetFields()[v-1]
		}
	}
	for _, i := range fields.bytes {
		bind[fields.fields[i].Name] = source.DeserializeBytes()
	}
	k := 0
	for _, i := range fields.sliceStringsSets {
		v := int(source.DeserializeUInteger())
		if v == 0 {
			bind[fields.fields[i].Name] = nil
		} else {
			e := fields.sets[k]
			values := make([]string, v)
			for j := 0; j < v; j++ {
				values[j] = e.GetFields()[source.DeserializeUInteger()-1]
			}
			bind[fields.fields[i].Name] = strings.Join(values, ",")
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		if source.DeserializeBool() {
			bind[fields.fields[i].Name] = source.DeserializeBool()
			continue
		}
		bind[fields.fields[i].Name] = nil
	}
	for _, i := range fields.floatsNullable {
		if source.DeserializeBool() {
			bind[fields.fields[i].Name] = source.DeserializeFloat()
			continue
		}
		bind[fields.fields[i].Name] = nil
	}
	for _, i := range fields.timesNullable {
		if source.DeserializeBool() {
			bind[fields.fields[i].Name] = time.Unix(source.DeserializeInteger()-timeStampSeconds, 0).Format(time.DateTime)
			continue
		}
		bind[fields.fields[i].Name] = nil
	}
	for _, i := range fields.datesNullable {
		if source.DeserializeBool() {
			bind[fields.fields[i].Name] = time.Unix(source.DeserializeInteger()-timeStampSeconds, 0).Format(time.DateOnly)
			continue
		}
		bind[fields.fields[i].Name] = nil
	}
	for j := range fields.structs {
		fillBindFromOneSource(c, bind, source, fields.structsFields[j], false)
	}
}

func fillBindFromTwoSources(c Context, bind, oldBind Bind, source, before *serializer, fields *tableFields, root bool) {
	if root {
		source.DeserializeUInteger()
		before.DeserializeUInteger()
	}
	for _, i := range fields.uintegers {
		v1 := source.DeserializeUInteger()
		v2 := before.DeserializeUInteger()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.refs {
		v1 := source.DeserializeUInteger()
		v2 := before.DeserializeUInteger()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			if v1 == 0 {
				bind[fields.fields[i].Name] = nil
			}
			oldBind[fields.fields[i].Name] = v2
			if v2 == 0 {
				oldBind[fields.fields[i].Name] = nil
			}
		}
	}
	for _, i := range fields.integers {
		v1 := source.DeserializeInteger()
		v2 := before.DeserializeInteger()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.booleans {
		v1 := source.DeserializeBool()
		v2 := before.DeserializeBool()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.floats {
		v1 := source.DeserializeFloat()
		v2 := before.DeserializeFloat()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.times {
		v1 := source.DeserializeInteger()
		v2 := before.DeserializeInteger()
		if v1 != v2 {
			if v1 == zeroDateSeconds {
				bind[fields.fields[i].Name] = "0000-00-00 00:00:00"
			} else {
				bind[fields.fields[i].Name] = time.Unix(v1-timeStampSeconds, 0).Format(time.DateTime)
			}
			if v2 == zeroDateSeconds {
				oldBind[fields.fields[i].Name] = "0000-00-00 00:00:00"
			} else {
				oldBind[fields.fields[i].Name] = time.Unix(v2-timeStampSeconds, 0).Format(time.DateTime)
			}
		}
	}
	for _, i := range fields.dates {
		v1 := source.DeserializeInteger()
		v2 := before.DeserializeInteger()
		if v1 != v2 {
			if v1 == zeroDateSeconds {
				bind[fields.fields[i].Name] = "0000-00-00"
			} else {
				bind[fields.fields[i].Name] = time.Unix(v1-timeStampSeconds, 0).Format(time.DateOnly)
			}
			if v2 == zeroDateSeconds {
				oldBind[fields.fields[i].Name] = "0000-00-00"
			} else {
				oldBind[fields.fields[i].Name] = time.Unix(v2-timeStampSeconds, 0).Format(time.DateOnly)
			}
		}
	}
	for _, i := range fields.strings {
		v1 := source.DeserializeString()
		v2 := before.DeserializeString()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.uintegersNullable {
		v1 := uint64(0)
		v2 := uint64(0)
		v1IsNil := source.DeserializeBool()
		v2IsNil := before.DeserializeBool()
		if v1IsNil {
			v1 = source.DeserializeUInteger()
		}
		if v2IsNil {
			v2 = before.DeserializeUInteger()
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
		v1IsNil := source.DeserializeBool()
		v2IsNil := before.DeserializeBool()
		if v1IsNil {
			v1 = source.DeserializeInteger()
		}
		if v2IsNil {
			v2 = before.DeserializeInteger()
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
	for z, i := range fields.stringsEnums {
		v1 := source.DeserializeUInteger()
		v2 := before.DeserializeUInteger()
		if v1 != v2 {
			bind[fields.fields[i].Name] = ""
			oldBind[fields.fields[i].Name] = ""
			if v1 > 0 {
				bind[fields.fields[i].Name] = fields.enums[z].GetFields()[v1-1]
			}
			if v2 > 0 {
				oldBind[fields.fields[i].Name] = fields.enums[z].GetFields()[v2-1]
			}
		}
	}
	for _, i := range fields.bytes {
		v1 := source.DeserializeBytes()
		v2 := before.DeserializeBytes()
		if !bytes.Equal(v1, v2) {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	k := 0
	for _, i := range fields.sliceStringsSets {
		v1 := int(source.DeserializeUInteger())
		v2 := int(before.DeserializeUInteger())
		if v1 != v2 {
			if v1 == 0 {
				bind[fields.fields[i].Name] = nil
			} else {
				e := fields.sets[k]
				values := make([]string, v1)
				for j := 0; j < v1; j++ {
					values[j] = e.GetFields()[source.DeserializeUInteger()-1]
				}
				bind[fields.fields[i].Name] = strings.Join(values, ",")
			}
			if v2 == 0 {
				oldBind[fields.fields[i].Name] = nil
			} else {
				e := fields.sets[k]
				values := make([]string, v2)
				for j := 0; j < v2; j++ {
					values[j] = e.GetFields()[source.DeserializeUInteger()-1]
				}
				oldBind[fields.fields[i].Name] = strings.Join(values, ",")
			}
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		v1 := false
		v2 := false
		v1IsNil := source.DeserializeBool()
		v2IsNil := before.DeserializeBool()
		if v1IsNil {
			v1 = source.DeserializeBool()
		}
		if v2IsNil {
			v2 = before.DeserializeBool()
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
	for _, i := range fields.floatsNullable {
		v1 := float64(0)
		v2 := float64(0)
		v1IsNil := source.DeserializeBool()
		v2IsNil := before.DeserializeBool()
		if v1IsNil {
			v1 = source.DeserializeFloat()
		}
		if v2IsNil {
			v2 = before.DeserializeFloat()
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
		v1 := int64(0)
		v2 := int64(0)
		v1IsNil := source.DeserializeBool()
		v2IsNil := before.DeserializeBool()
		if v1IsNil {
			v1 = source.DeserializeInteger()
		}
		if v2IsNil {
			v2 = before.DeserializeInteger()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			bind[fields.fields[i].Name] = nil
			oldBind[fields.fields[i].Name] = nil
			if !v1IsNil {
				bind[fields.fields[i].Name] = time.Unix(v1-timeStampSeconds, 0).Format(time.DateTime)
			}
			if !v2IsNil {
				oldBind[fields.fields[i].Name] = time.Unix(v2-timeStampSeconds, 0).Format(time.DateTime)
			}
		}
	}
	for _, i := range fields.datesNullable {
		v1 := int64(0)
		v2 := int64(0)
		v1IsNil := source.DeserializeBool()
		v2IsNil := before.DeserializeBool()
		if v1IsNil {
			v1 = source.DeserializeInteger()
		}
		if v2IsNil {
			v2 = before.DeserializeInteger()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			bind[fields.fields[i].Name] = nil
			oldBind[fields.fields[i].Name] = nil
			if !v1IsNil {
				bind[fields.fields[i].Name] = time.Unix(v1-timeStampSeconds, 0).Format(time.DateOnly)
			}
			if !v2IsNil {
				oldBind[fields.fields[i].Name] = time.Unix(v2-timeStampSeconds, 0).Format(time.DateOnly)
			}
		}
	}
	for j := range fields.structs {
		fillBindFromTwoSources(c, bind, oldBind, source, before, fields.structsFields[j], false)
	}
}
