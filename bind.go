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

type Bind map[string]interface{}

type BindError struct {
	Field   string
	Message string
}

func (b *BindError) Error() string {
	return "[" + b.Field + "] " + b.Message
}

func (b *BindError) MessageMessage() string {
	return b.Message
}

func (b Bind) Get(key string) interface{} {
	return b[key]
}

func (m *insertableEntity[E]) getBind() (Bind, error) {
	bind := Bind{}
	if m.entity.GetID() > 0 {
		bind["ID"] = m.entity.GetID()
	}
	err := fillBindFromOneSource(m.c, bind, m.value.Elem(), GetEntitySchema[E](m.c).getFields(), "")
	if err != nil {
		return nil, err
	}
	return bind, nil
}

func (e *editableEntity[E]) getBind() (new, old Bind, err error) {
	new = Bind{}
	old = Bind{}
	fillBindFromTwoSources(e.c, new, old, e.value, reflect.ValueOf(e.source), GetEntitySchema[E](e.c).getFields())
	return
}

func fillBindFromOneSource(c Context, bind Bind, source reflect.Value, fields *tableFields, prefix string) error {
	for _, i := range fields.uIntegers {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Uint()
	}
	for k, i := range fields.references {
		f := source.Field(i)
		required := fields.referencesRequired[k]
		if f.IsNil() {
			if required {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "nil value not allowed"}
			}
			f.SetZero()
		} else {
			reference := f.Interface().(referenceInterface)
			if required && reference.GetID() == 0 {
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "ID zero not allowed"}
			}
			bind[prefix+fields.fields[i].Name] = reference.GetID()
		}
	}
	for _, i := range fields.integers {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Int()
	}
	for _, i := range fields.booleans {
		bind[prefix+fields.fields[i].Name] = source.Field(i).Bool()
	}
	for k, i := range fields.floats {
		f := source.Field(i)
		v := f.Float()
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
			bind[prefix+fields.fields[i].Name] = nil
		} else {
			bind[prefix+fields.fields[i].Name] = v
		}
	}
	for _, i := range fields.uIntegersNullable {
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
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty value not allowed"}
			} else {
				bind[prefix+fields.fields[i].Name] = nil
			}
			continue
		}
		if !slices.Contains(def.GetFields(), val) {
			return &BindError{Field: prefix + fields.fields[i].Name, Message: fmt.Sprintf("invalid value: %s", val)}
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
				return &BindError{Field: prefix + fields.fields[i].Name, Message: "empty value not allowed"}
			} else {
				bind[prefix+fields.fields[i].Name] = nil
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
			bind[prefix+fields.fields[i].Name] = f.Elem().Bool()
			continue
		}
		bind[prefix+fields.fields[i].Name] = nil
	}
	for k, i := range fields.floatsNullable {
		f := source.Field(i)
		if !f.IsNil() {
			v := f.Elem().Float()
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
		bind[prefix+fields.fields[i].Name] = nil
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
		bind[prefix+fields.fields[i].Name] = nil
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
		bind[prefix+fields.fields[i].Name] = nil
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

func fillBindFromTwoSources(c Context, bind, oldBind Bind, source, before reflect.Value, fields *tableFields) {
	for _, i := range fields.uIntegers {
		v1 := source.Field(i).Uint()
		v2 := before.Field(i).Uint()
		if v1 != v2 {
			bind[fields.fields[i].Name] = v1
			oldBind[fields.fields[i].Name] = v2
		}
	}
	for k, i := range fields.references {
		// TODO
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
				//return &BindError{Field: fields.fields[i].Name, Message: "nil value not allowed"}
			}
		} else if isRequired {
			//return &BindError{Field: fields.fields[i].Name, Message: "ID zero not allowed"}
		}
		if !v2IsNil {
			v2 = f2.Interface().(referenceInterface).GetID()
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
		v1 := source.Field(i).Interface().(time.Time)
		v2 := before.Field(i).Interface().(time.Time)
		if v1.Unix() != v2.Unix() {
			bind[fields.fields[i].Name] = v1.Format(time.DateTime)
			oldBind[fields.fields[i].Name] = v2.Format(time.DateTime)
		}
	}
	for _, i := range fields.dates {
		v1 := source.Field(i).Interface().(time.Time)
		v1 = time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
		v2 := before.Field(i).Interface().(time.Time)
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
	for _, i := range fields.uIntegersNullable {
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
			v1 = f1.Elem().Interface().(time.Time)
		}
		if !v2IsNil {
			v2 = f2.Elem().Interface().(time.Time)
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
			v1 = f1.Elem().Interface().(time.Time)
			v1 = time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
		}
		if !v2IsNil {
			v2 = f2.Elem().Interface().(time.Time)
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

func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}
