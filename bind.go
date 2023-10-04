package beeorm

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

const zeroDateSeconds = 31622400
const timeStampSeconds = 62167219200
const nullAsString = "NULL"
const zeroAsString = "0"
const zeroTimeAsString = "0001-01-01 00:00:00"
const zeroDateAsString = "0001-01-01"

type Bind map[string]string

type BindError struct {
	Field   string
	Message string
}

func (b *BindError) Error() string {
	return "[" + b.Field + "] " + b.Message
}

type DuplicatedKeyBindError struct {
	Index   string
	ID      uint64
	Columns []string
}

func (d *DuplicatedKeyBindError) Error() string {
	return "duplicated value for unique index '" + d.Index + "'"
}

func (b Bind) Get(key string) interface{} {
	return b[key]
}

func (m *insertableEntity[E]) getBind() (Bind, error) {
	bind := Bind{}
	bind["ID"] = strconv.FormatUint(m.id, 10)
	schema := m.Schema()
	err := fillBindFromOneSource(m.c, bind, m.value.Elem(), schema.getFields(), "")
	if err != nil {
		return nil, err
	}
	return bind, nil
}

func (e *editableEntity[E]) getBind() (new, old Bind, err error) {
	new = Bind{}
	old = Bind{}
	err = fillBindFromTwoSources(e.c, new, old, e.value.Elem(), reflect.ValueOf(e.source).Elem(), GetEntitySchema[E](e.c).getFields(), "")
	return
}

func (r *removableEntity[E]) getOldBind() (bind Bind, err error) {
	bind = Bind{}
	schema := r.Schema()
	err = fillBindFromOneSource(r.c, bind, reflect.ValueOf(r.source).Elem(), schema.getFields(), "")
	if err != nil {
		return nil, err
	}
	return bind, nil
}

func fillBindFromOneSource(c Context, bind Bind, source reflect.Value, fields *tableFields, prefix string) error {
	for _, i := range fields.uIntegers {
		v := source.Field(i).Uint()
		if v > 0 {
			bind[prefix+fields.fields[i].Name] = strconv.FormatUint(v, 10)
		} else {
			bind[prefix+fields.fields[i].Name] = zeroAsString
		}
	}
	for k, i := range fields.references {
		f := source.Field(i)
		required := fields.referencesRequired[k]
		if f.IsNil() {
			if required {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "nil value not allowed"}
			}
			f.SetZero()
			bind[prefix+fields.fields[i].Name] = nullAsString
		} else {
			reference := f.Interface().(referenceInterface)
			if required && reference.GetID() == 0 {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "ID zero not allowed"}
			}
			bind[prefix+fields.fields[i].Name] = strconv.FormatUint(reference.GetID(), 10)
		}
	}
	for _, i := range fields.integers {
		v := source.Field(i).Int()
		if v != 0 {
			bind[prefix+fields.fields[i].Name] = strconv.FormatInt(v, 10)
		} else {
			bind[prefix+fields.fields[i].Name] = zeroAsString
		}
	}
	for _, i := range fields.booleans {
		if source.Field(i).Bool() {
			bind[prefix+fields.fields[i].Name] = "1"
		} else {
			bind[prefix+fields.fields[i].Name] = zeroAsString
		}

	}
	for k, i := range fields.floats {
		f := source.Field(i)
		v := f.Float()
		if v == 0 {
			bind[prefix+fields.fields[i].Name] = zeroAsString
			continue
		}
		if fields.floatsUnsigned[k] && v < 0 {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "negative value not allowed"}
		}
		roundV := roundFloat(v, fields.floatsPrecision[k])
		val := strconv.FormatFloat(roundV, 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		decimalSize := fields.floatsDecimalSize[k]
		if decimalSize != -1 && strings.Index(val, ".") > decimalSize {
			return &BindError{Field: prefix + fields.fields[i].Name,
				Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
		}
		bind[prefix+fields.fields[i].Name] = val
		if v != roundV {
			f.SetFloat(roundV)
		}
	}
	for _, i := range fields.times {
		f := source.Field(i)
		v := f.Interface().(time.Time)
		if v.Location() != time.UTC {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
		}
		v2 := time.Date(v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), 0, time.UTC)
		if v != v2 {
			f.Set(reflect.ValueOf(v2))
		}
		bind[prefix+fields.fields[i].Name] = v2.Format(time.DateTime)
	}
	for _, i := range fields.dates {
		f := source.Field(i)
		v := f.Interface().(time.Time)
		if v.Location() != time.UTC {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
		}
		v2 := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC)
		if v != v2 {
			f.Set(reflect.ValueOf(v2))
		}
		bind[prefix+fields.fields[i].Name] = v2.Format(time.DateOnly)
	}
	for k, i := range fields.strings {
		v := source.Field(i).String()
		if len(v) > fields.stringMaxLengths[k] {
			return &BindError{Field: prefix + fields.fields[i].Name,
				Message: fmt.Sprintf("text too long, max %d allowed", fields.stringMaxLengths[k])}
		}
		if v == "" {
			isRequired := fields.stringsRequired[k]
			if isRequired {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty string not allowed"}
			}
			bind[prefix+fields.fields[i].Name] = nullAsString
		} else {
			bind[prefix+fields.fields[i].Name] = v
		}
	}
	for _, i := range fields.uIntegersNullable {
		f := source.Field(i)
		if !f.IsNil() {
			bind[prefix+fields.fields[i].Name] = strconv.FormatUint(f.Elem().Uint(), 10)
			continue
		}
		bind[fields.fields[i].Name] = nullAsString
	}
	for _, i := range fields.integersNullable {
		f := source.Field(i)
		if !f.IsNil() {
			bind[prefix+fields.fields[i].Name] = strconv.FormatInt(f.Elem().Int(), 10)
			continue
		}
		bind[prefix+fields.fields[i].Name] = nullAsString
	}
	for k, i := range fields.stringsEnums {
		val := source.Field(i).String()
		def := fields.enums[k]
		if val == "" {
			if def.required {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty value not allowed"}
			} else {
				bind[prefix+fields.fields[i].Name] = nullAsString
			}
			continue
		}
		if !slices.Contains(def.GetFields(), val) {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: fmt.Sprintf("invalid value: %s", val)}
		}
		bind[prefix+fields.fields[i].Name] = val
	}
	for _, i := range fields.bytes {
		v := source.Field(i).Bytes()
		if v == nil {
			bind[prefix+fields.fields[i].Name] = nullAsString
		} else {
			bind[prefix+fields.fields[i].Name] = string(v)
		}
	}
	for k, i := range fields.sliceStringsSets {
		f := source.Field(i)
		def := fields.sets[k]
		if f.IsNil() || f.Len() == 0 {
			if def.required {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty value not allowed"}
			} else {
				bind[prefix+fields.fields[i].Name] = nullAsString
			}
		} else {
			s := c.getStringBuilder()
			for j := 0; j < f.Len(); j++ {
				v := f.Index(j).String()
				if !slices.Contains(def.GetFields(), v) {
					return &BindError{Field: prefix + fields.fields[i].Name, Message: fmt.Sprintf("invalid value: %s", v)}
				}
				if j > 0 {
					s.WriteString(",")
				}
				s.WriteString(v)
			}
			bind[prefix+fields.fields[i].Name] = s.String()
		}
	}
	for _, i := range fields.booleansNullable {
		f := source.Field(i)
		if !f.IsNil() {
			if f.Elem().Bool() {
				bind[prefix+fields.fields[i].Name] = "1"
			} else {
				bind[prefix+fields.fields[i].Name] = zeroAsString
			}
			continue
		}
		bind[prefix+fields.fields[i].Name] = nullAsString
	}
	for k, i := range fields.floatsNullable {
		f := source.Field(i)
		if !f.IsNil() {
			v := f.Elem().Float()
			if v == 0 {
				bind[prefix+fields.fields[i].Name] = zeroAsString
				continue
			}
			if fields.floatsNullableUnsigned[k] && v < 0 {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "negative value not allowed"}
			}
			roundV := roundFloat(v, fields.floatsNullablePrecision[k])
			val := strconv.FormatFloat(roundV, 'f', fields.floatsNullablePrecision[k], fields.floatsNullableSize[k])
			decimalSize := fields.floatsNullableDecimalSize[k]
			if decimalSize != -1 && strings.Index(val, ".") > decimalSize {
				return &BindError{Field: prefix + fields.fields[i].Name,
					Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
			}
			bind[prefix+fields.fields[i].Name] = val
			if v != roundV {
				f.Elem().SetFloat(roundV)
			}
			continue
		}
		bind[prefix+fields.fields[i].Name] = nullAsString
	}
	for _, i := range fields.timesNullable {
		f := source.Field(i)
		if !f.IsNil() {
			v := f.Elem().Interface().(time.Time)
			if v.Location() != time.UTC {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
			}
			v2 := time.Date(v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), 0, time.UTC)
			if v != v2 {
				f.Set(reflect.ValueOf(&v2))
			}
			bind[prefix+fields.fields[i].Name] = v2.Format(time.DateTime)
			continue
		}
		bind[prefix+fields.fields[i].Name] = nullAsString
	}
	for _, i := range fields.datesNullable {
		f := source.Field(i)
		if !f.IsNil() {
			v := f.Elem().Interface().(time.Time)
			if v.Location() != time.UTC {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
			}
			v2 := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC)
			if v != v2 {
				f.Set(reflect.ValueOf(&v2))
			}
			bind[prefix+fields.fields[i].Name] = v2.Format(time.DateOnly)
			continue
		}
		bind[prefix+fields.fields[i].Name] = nullAsString
	}
	for j, i := range fields.structs {
		sub := fields.structsFields[j]
		err := fillBindFromOneSource(c, bind, source.Field(i), sub, prefix+sub.prefix)
		if err != nil {
			return err
		}
	}
	return nil
}

func fillBindFromTwoSources(c Context, bind, oldBind Bind, source, before reflect.Value, fields *tableFields, prefix string) error {
	for _, i := range fields.uIntegers {
		v1 := source.Field(i).Uint()
		v2 := before.Field(i).Uint()
		if v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1 == 0 {
				bind[name] = zeroAsString
			} else {
				bind[name] = strconv.FormatUint(v1, 10)
			}
			if v2 == 0 {
				oldBind[name] = zeroAsString
			} else {
				oldBind[name] = strconv.FormatUint(v2, 10)
			}
		} else if fields.forcedOldBid[i] {
			if v2 == 0 {
				oldBind[prefix+fields.fields[i].Name] = zeroAsString
			} else {
				oldBind[prefix+fields.fields[i].Name] = strconv.FormatUint(v2, 10)
			}
		}
	}
	for k, i := range fields.references {
		v1 := uint64(0)
		v2 := uint64(0)
		f1 := source.Field(i)
		f2 := before.Field(i)
		isRequired := fields.referencesRequired[k]
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Interface().(referenceInterface).GetID()
			if isRequired && v1 == 0 {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "nil value not allowed"}
			}
		} else if isRequired {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "nil value not allowed"}
		}
		if !v2IsNil {
			v2 = f2.Interface().(referenceInterface).GetID()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = strconv.FormatUint(v1, 10)
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = strconv.FormatUint(v2, 10)
			}
		} else if fields.forcedOldBid[i] {
			if v2IsNil {
				oldBind[prefix+fields.fields[i].Name] = nullAsString
			} else {
				oldBind[prefix+fields.fields[i].Name] = strconv.FormatUint(v2, 10)
			}
		}
	}
	for _, i := range fields.integers {
		v1 := source.Field(i).Int()
		v2 := before.Field(i).Int()
		if v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1 == 0 {
				bind[name] = zeroAsString
			} else {
				bind[name] = strconv.FormatInt(v1, 10)
			}
			if v2 == 0 {
				oldBind[name] = zeroAsString
			} else {
				oldBind[name] = strconv.FormatInt(v2, 10)
			}
		} else if fields.forcedOldBid[i] {
			if v2 == 0 {
				oldBind[prefix+fields.fields[i].Name] = zeroAsString
			} else {
				oldBind[prefix+fields.fields[i].Name] = strconv.FormatInt(v2, 10)
			}
		}
	}
	for _, i := range fields.booleans {
		v1 := source.Field(i).Bool()
		v2 := before.Field(i).Bool()
		if v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1 {
				bind[name] = "1"
			} else {
				bind[name] = zeroAsString
			}
			if v2 {
				oldBind[name] = "1"
			} else {
				oldBind[name] = zeroAsString
			}
		} else if fields.forcedOldBid[i] {
			if v2 {
				oldBind[prefix+fields.fields[i].Name] = "1"
			} else {
				oldBind[prefix+fields.fields[i].Name] = zeroAsString
			}
		}
	}
	for k, i := range fields.floats {
		f := source.Field(i)
		v := f.Float()
		if fields.floatsUnsigned[k] && v < 0 {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "negative value not allowed"}
		}
		roundV := roundFloat(v, fields.floatsPrecision[k])
		v1 := strconv.FormatFloat(roundV, 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		if v != roundV {
			f.SetFloat(roundV)
		}
		decimalSize := fields.floatsDecimalSize[k]
		if decimalSize != -1 && strings.Index(v1, ".") > decimalSize {
			return &BindError{Field: prefix + fields.fields[i].Name,
				Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
		}
		v2Float := before.Field(i).Float()
		v2 := strconv.FormatFloat(v2Float, 'f', fields.floatsPrecision[k], fields.floatsSize[k])
		if v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v == 0 {
				bind[name] = zeroAsString
			} else {
				bind[name] = v1
			}
			if v2Float == 0 {
				oldBind[name] = zeroAsString
			} else {
				oldBind[name] = v2
			}
		} else if fields.forcedOldBid[i] {
			oldBind[prefix+fields.fields[i].Name] = v2
		}
	}
	for _, i := range fields.times {
		f := source.Field(i)
		v1 := f.Interface().(time.Time)
		if v1.Location() != time.UTC {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
		}
		v1Check := time.Date(v1.Year(), v1.Month(), v1.Day(), v1.Hour(), v1.Minute(), v1.Second(), 0, time.UTC)
		if v1 != v1Check {
			f.Set(reflect.ValueOf(v1Check))
			v1 = v1Check
		}
		v2 := before.Field(i).Interface().(time.Time)
		if v1.Unix() != v2.Unix() {
			name := prefix + fields.fields[i].Name
			bind[name] = v1.Format(time.DateTime)
			oldBind[name] = v2.Format(time.DateTime)
		} else if fields.forcedOldBid[i] {
			oldBind[prefix+fields.fields[i].Name] = v2.Format(time.DateTime)
		}
	}
	for _, i := range fields.dates {
		f := source.Field(i)
		v1 := f.Interface().(time.Time)
		if v1.Location() != time.UTC {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
		}
		v1Check := time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
		if v1 != v1Check {
			f.Set(reflect.ValueOf(v1Check))
			v1 = v1Check
		}
		v2 := before.Field(i).Interface().(time.Time)
		if v1.Unix() != v2.Unix() {
			name := prefix + fields.fields[i].Name
			bind[name] = v1.Format(time.DateOnly)
			oldBind[name] = v2.Format(time.DateOnly)
		} else if fields.forcedOldBid[i] {
			oldBind[prefix+fields.fields[i].Name] = v2.Format(time.DateOnly)
		}
	}
	for k, i := range fields.strings {
		v1 := source.Field(i).String()
		if len(v1) > fields.stringMaxLengths[k] {
			return &BindError{Field: prefix + fields.fields[i].Name,
				Message: fmt.Sprintf("text too long, max %d allowed", fields.stringMaxLengths[k])}
		}
		if v1 == "" {
			isRequired := fields.stringsRequired[k]
			if isRequired {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty string not allowed"}
			}
		}
		v2 := before.Field(i).String()
		if v1 != v2 {
			name := prefix + fields.fields[i].Name
			bind[name] = v1
			oldBind[name] = v2
			if fields.stringsRequired[k] {
				if v1 == "" {
					bind[name] = nullAsString
				}
				if v2 == "" {
					oldBind[name] = nullAsString
				}
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			oldBind[name] = v2
			if v2 == "" {
				oldBind[name] = nullAsString
			}
		}
	}
	for _, i := range fields.uIntegersNullable {
		v1 := uint64(0)
		v2 := uint64(0)
		f1 := source.Field(i)
		f2 := before.Field(i)
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			v1 = f1.Elem().Uint()
		}
		if !v2IsNil {
			v2 = f2.Elem().Uint()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = strconv.FormatUint(v1, 10)
			}
			if v2IsNil {
				oldBind[prefix+fields.fields[i].Name] = nullAsString
			} else {
				oldBind[name] = strconv.FormatUint(v2, 10)
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[prefix+fields.fields[i].Name] = nullAsString
			} else {
				oldBind[name] = strconv.FormatUint(v2, 10)
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
			v1 = f1.Elem().Int()
		}
		if !v2IsNil {
			v2 = f2.Elem().Int()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = strconv.FormatInt(v1, 10)
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = strconv.FormatInt(v2, 10)
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = strconv.FormatInt(v2, 10)
			}
		}
	}
	for k, i := range fields.stringsEnums {
		v1 := source.Field(i).String()
		v2 := before.Field(i).String()
		def := fields.enums[k]
		if v1 == "" {
			if def.required {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty value not allowed"}
			}
		} else if !slices.Contains(def.GetFields(), v1) {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: fmt.Sprintf("invalid value: %s", v1)}
		}
		if v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1 == "" && !def.required {
				bind[name] = nullAsString
			} else {
				bind[name] = v1
			}
			if v2 == "" && !def.required {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2 == "" && !def.required {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2
			}
		}
	}
	for _, i := range fields.bytes {
		v1 := source.Field(i).Bytes()
		v2 := before.Field(i).Bytes()
		if !bytes.Equal(v1, v2) {
			name := prefix + fields.fields[i].Name
			if v1 == nil {
				bind[name] = nullAsString
			} else {
				bind[name] = string(v1)
			}
			if v2 == nil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = string(v2)
			}
		} else if fields.forcedOldBid[i] {
			if v2 == nil {
				oldBind[prefix+fields.fields[i].Name] = nullAsString
			} else {
				oldBind[prefix+fields.fields[i].Name] = string(v2)
			}
		}
	}
	for k, i := range fields.sliceStringsSets {
		f1 := source.Field(i)
		f2 := before.Field(i)
		def := fields.sets[k]
		if f1.IsNil() || f1.Len() == 0 {
			if def.required {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty value not allowed"}
			}
		}
		var v1 []string
		var v2 []string
		v1IsNil := f1.IsNil()
		v2IsNil := f2.IsNil()
		if !v1IsNil {
			for j := 0; j < f1.Len(); j++ {
				v := f1.Index(j).String()
				if !slices.Contains(def.GetFields(), v) {
					return &BindError{Field: prefix + fields.fields[i].Name, Message: fmt.Sprintf("invalid value: %s", v)}
				}
				v1 = append(v1, v)
			}
		}
		if !v2IsNil {
			for j := 0; j < f2.Len(); j++ {
				v := f2.Index(j).String()
				v2 = append(v2, v)
			}
		}
		if v1IsNil != v2IsNil || !compareSlices(v1, v2) {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = strings.Join(v1, ",")
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = strings.Join(v2, ",")
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = strings.Join(v2, ",")
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
			v1 = f1.Elem().Bool()
		}
		if !v2IsNil {
			v2 = f2.Elem().Bool()
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1 {
				bind[name] = "1"
			} else {
				bind[name] = zeroAsString
			}
			if v2 {
				oldBind[name] = "1"
			} else {
				oldBind[name] = zeroAsString
			}
			if v1IsNil {
				bind[name] = nullAsString
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[name] = nullAsString
			} else if v2 {
				oldBind[name] = "1"
			} else {
				oldBind[name] = zeroAsString
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
			f := f1.Elem().Float()
			if fields.floatsNullableUnsigned[k] && f < 0 {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "negative value not allowed"}
			}
			roundV := roundFloat(f, fields.floatsNullablePrecision[k])
			v1 = strconv.FormatFloat(roundV, 'f', fields.floatsNullablePrecision[k], fields.floatsNullableSize[k])
			if f != roundV {
				if fields.floatsNullableSize[k] == 32 {
					roundV32 := float32(roundV)
					f1.Set(reflect.ValueOf(&roundV32))
				} else {
					f1.Set(reflect.ValueOf(&roundV))
				}
			}
			decimalSize := fields.floatsNullableDecimalSize[k]
			if decimalSize != -1 && strings.Index(v1, ".") > decimalSize {
				return &BindError{Field: prefix + fields.fields[i].Name,
					Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
			}
		}
		if !v2IsNil {
			v2 = strconv.FormatFloat(f2.Elem().Float(), 'f', fields.floatsNullablePrecision[k], fields.floatsNullableSize[k])
		}
		if v1IsNil != v2IsNil || v1 != v2 {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = v1
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2
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
			v1 = f1.Elem().Interface().(time.Time)
			if v1.Location() != time.UTC {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
			}
			vFinal := time.Date(v1.Year(), v1.Month(), v1.Day(), v1.Hour(), v1.Minute(), v1.Second(), 0, time.UTC)
			if vFinal != v1 {
				f1.Set(reflect.ValueOf(&vFinal))
				v1 = vFinal
			}
		}
		if !v2IsNil {
			v2 = f2.Elem().Interface().(time.Time)
		}
		if v1IsNil != v2IsNil || v1.Unix() != v2.Unix() {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = v1.Format(time.DateTime)
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2.Format(time.DateTime)
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2.Format(time.DateTime)
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
			v1 = f1.Elem().Interface().(time.Time)
			if v1.Location() != time.UTC {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "time must be in UTC location"}
			}
			vFinal := time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
			if vFinal != v1 {
				f1.Set(reflect.ValueOf(&vFinal))
				v1 = vFinal
			}
		}
		if !v2IsNil {
			v2 = f2.Elem().Interface().(time.Time)
		}
		if v1IsNil != v2IsNil || v1.Unix() != v2.Unix() {
			name := prefix + fields.fields[i].Name
			if v1IsNil {
				bind[name] = nullAsString
			} else {
				bind[name] = v1.Format(time.DateOnly)
			}
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2.Format(time.DateOnly)
			}
		} else if fields.forcedOldBid[i] {
			name := prefix + fields.fields[i].Name
			if v2IsNil {
				oldBind[name] = nullAsString
			} else {
				oldBind[name] = v2.Format(time.DateOnly)
			}
		}
	}
	for j, i := range fields.structs {
		sub := fields.structsFields[j]
		err := fillBindFromTwoSources(c, bind, oldBind, source.Field(i), before.Field(i), sub, prefix+sub.prefix)
		if err != nil {
			return err
		}
	}
	return nil
}

func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func compareSlices(left, right []string) bool {
	for _, val := range left {
		if !slices.Contains(right, val) {
			return false
		}
	}
	for _, val := range right {
		if !slices.Contains(left, val) {
			return false
		}
	}
	return true
}

func convertBindToRedisValue(bind Bind, schema EntitySchema) []interface{} {
	values := make([]interface{}, len(bind)+1)
	values[0] = schema.getStructureHash()
	for i, column := range schema.GetColumns() {
		values[i+1] = convertBindValueToRedisValue(bind[column])
	}
	return values
}

func convertBindValueToRedisValue(value string) string {
	if value == nullAsString || value == zeroAsString || value == zeroTimeAsString || value == zeroDateAsString {
		return ""
	}
	return value
}
