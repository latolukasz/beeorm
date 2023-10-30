// Package beeorm keeps main code od BeeORM
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

func (b Bind) Get(key string) any {
	return b[key]
}

func (m *insertableEntity[E]) getBind() (Bind, error) {
	bind := Bind{}
	bind["ID"] = strconv.FormatUint(m.id, 10)
	schema := m.Schema()
	err := fillBindFromOneSource(m.c, bind, m.value.Elem(), schema.fields, "")
	if err != nil {
		return nil, err
	}
	return bind, nil
}

func (e *editableEntity[E]) getBind() (newBind, oldBind Bind, err error) {
	newBind = Bind{}
	oldBind = Bind{}
	err = fillBindFromTwoSources(e.c, newBind, oldBind, e.value.Elem(), reflect.ValueOf(e.source).Elem(), getEntitySchema[E](e.c).fields, "")
	return
}

func (r *removableEntity[E]) getOldBind() (bind Bind, err error) {
	bind = Bind{}
	schema := r.Schema()
	err = fillBindFromOneSource(r.c, bind, reflect.ValueOf(r.source).Elem(), schema.fields, "")
	if err != nil {
		return nil, err
	}
	return bind, nil
}

func fillBindForUint(bind Bind, v uint64, column string) {
	if v > 0 {
		bind[column] = strconv.FormatUint(v, 10)
	} else {
		bind[column] = zeroAsString
	}
}

func fillBindForReference(bind Bind, f reflect.Value, required bool, column string) error {
	if f.IsNil() {
		if required {
			return &BindError{Field: column, Message: "nil value not allowed"}
		}
		f.SetZero()
		bind[column] = nullAsString
	} else {
		reference := f.Interface().(referenceInterface)
		if required && reference.getID() == 0 {
			return &BindError{Field: column, Message: "ID zero not allowed"}
		}
		bind[column] = strconv.FormatUint(reference.getID(), 10)
	}
	return nil
}

func fillBindForInt(bind Bind, v int64, column string) {
	if v != 0 {
		bind[column] = strconv.FormatInt(v, 10)
	} else {
		bind[column] = zeroAsString
	}
}

func fillBindForBool(bind Bind, v bool, column string) {
	if v {
		bind[column] = "1"
	} else {
		bind[column] = zeroAsString
	}
}

func fillBindForFloat(bind Bind, f reflect.Value, column string, floatsPrecision, floatsSize, floatsDecimalSize int, floatsUnsigned bool) error {
	v := f.Float()
	if v == 0 {
		bind[column] = zeroAsString
		return nil
	}
	if floatsUnsigned && v < 0 {
		return &BindError{Field: column, Message: "negative value not allowed"}
	}
	roundV := roundFloat(v, floatsPrecision)
	val := strconv.FormatFloat(roundV, 'f', floatsPrecision, floatsSize)
	decimalSize := floatsDecimalSize
	if decimalSize != -1 && strings.Index(val, ".") > decimalSize {
		return &BindError{Field: column,
			Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
	}
	bind[column] = val
	if v != roundV {
		f.SetFloat(roundV)
	}
	return nil
}

func fillBindForTime(bind Bind, f reflect.Value, column string) error {
	v := f.Interface().(time.Time)
	if v.Location() != time.UTC {
		return &BindError{Field: column, Message: "time must be in UTC location"}
	}
	v2 := time.Date(v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), 0, time.UTC)
	if v != v2 {
		f.Set(reflect.ValueOf(v2))
	}
	bind[column] = v2.Format(time.DateTime)
	return nil
}

func fillBindForDate(bind Bind, f reflect.Value, column string) error {
	v := f.Interface().(time.Time)
	if v.Location() != time.UTC {
		return &BindError{Field: column, Message: "time must be in UTC location"}
	}
	v2 := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC)
	if v != v2 {
		f.Set(reflect.ValueOf(v2))
	}
	bind[column] = v2.Format(time.DateOnly)
	return nil
}

func fillBindForString(bind Bind, f reflect.Value, column string, stringMaxLengths int, isRequired bool) error {
	v := f.String()
	if len(v) > stringMaxLengths {
		return &BindError{Field: column,
			Message: fmt.Sprintf("text too long, max %d allowed", stringMaxLengths)}
	}
	if v == "" {
		if isRequired {
			return &BindError{Field: column, Message: "empty string not allowed"}
		}
		bind[column] = nullAsString
	} else {
		bind[column] = v
	}
	return nil
}

func fillBindForUIntegersNullable(bind Bind, f reflect.Value, column string) {
	if !f.IsNil() {
		bind[column] = strconv.FormatUint(f.Elem().Uint(), 10)
		return
	}
	bind[column] = nullAsString
}

func fillBindForIntegersNullable(bind Bind, f reflect.Value, column string) {
	if !f.IsNil() {
		bind[column] = strconv.FormatInt(f.Elem().Int(), 10)
		return
	}
	bind[column] = nullAsString
}

func fillBindForBytes(bind Bind, f reflect.Value, column string) {
	v := f.Bytes()
	if v == nil {
		bind[column] = nullAsString
	} else {
		bind[column] = string(v)
	}
}

func fillBindForEnums(bind Bind, f reflect.Value, def *enumDefinition, column string) error {
	val := f.String()
	if val == "" {
		if def.required {
			return &BindError{Field: column, Message: "empty value not allowed"}
		}
		bind[column] = nullAsString
		return nil
	}
	if !slices.Contains(def.GetFields(), val) {
		return &BindError{Field: column, Message: fmt.Sprintf("invalid value: %s", val)}
	}
	bind[column] = val
	return nil
}

func fillBindForSets(bind Bind, f reflect.Value, def *enumDefinition, column string) error {
	if f.IsNil() || f.Len() == 0 {
		if def.required {
			return &BindError{Field: column, Message: "empty value not allowed"}
		}
		bind[column] = nullAsString
		return nil
	}
	s := ""
	for j := 0; j < f.Len(); j++ {
		v := f.Index(j).String()
		if !slices.Contains(def.GetFields(), v) {
			return &BindError{Field: column, Message: fmt.Sprintf("invalid value: %s", v)}
		}
		if j > 0 {
			s += ","
		}
		s += v
	}
	bind[column] = s
	return nil
}

func fillBindForBooleansNullable(bind Bind, f reflect.Value, column string) {
	if !f.IsNil() {
		if f.Elem().Bool() {
			bind[column] = "1"
		} else {
			bind[column] = zeroAsString
		}
		return
	}
	bind[column] = nullAsString
}

func fillBindForFloatsNullable(bind Bind, f reflect.Value, column string, unsigned bool, precision, size, decimalSize int) error {
	if !f.IsNil() {
		v := f.Elem().Float()
		if v == 0 {
			bind[column] = zeroAsString
			return nil
		}
		if unsigned && v < 0 {
			return &BindError{Field: column, Message: "negative value not allowed"}
		}
		roundV := roundFloat(v, precision)
		val := strconv.FormatFloat(roundV, 'f', precision, size)
		if decimalSize != -1 && strings.Index(val, ".") > decimalSize {
			return &BindError{Field: column,
				Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
		}
		bind[column] = val
		if v != roundV {
			f.Elem().SetFloat(roundV)
		}
		return nil
	}
	bind[column] = nullAsString
	return nil
}

func fillBindForTimesNullable(bind Bind, f reflect.Value, column string) error {
	if !f.IsNil() {
		v := f.Elem().Interface().(time.Time)
		if v.Location() != time.UTC {
			return &BindError{Field: column, Message: "time must be in UTC location"}
		}
		v2 := time.Date(v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), 0, time.UTC)
		if v != v2 {
			f.Set(reflect.ValueOf(&v2))
		}
		bind[column] = v2.Format(time.DateTime)
		return nil
	}
	bind[column] = nullAsString
	return nil
}

func fillBindForDatesNullable(bind Bind, f reflect.Value, column string) error {
	if !f.IsNil() {
		v := f.Elem().Interface().(time.Time)
		if v.Location() != time.UTC {
			return &BindError{Field: column, Message: "time must be in UTC location"}
		}
		v2 := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC)
		if v != v2 {
			f.Set(reflect.ValueOf(&v2))
		}
		bind[column] = v2.Format(time.DateOnly)
		return nil
	}
	bind[column] = nullAsString
	return nil
}

func fillBindFromOneSource(c Context, bind Bind, source reflect.Value, fields *tableFields, prefix string) error {
	for _, i := range fields.uIntegers {
		fillBindForUint(bind, source.Field(i).Uint(), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.uIntegersArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForUint(bind, f.Index(j).Uint(), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.references {
		f := source.Field(i)
		required := fields.referencesRequired[k]
		err := fillBindForReference(bind, f, required, prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for k, i := range fields.referencesArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			required := fields.referencesRequiredArray[k]
			err := fillBindForReference(bind, f.Index(j), required, prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.integers {
		fillBindForInt(bind, source.Field(i).Int(), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.integersArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForInt(bind, f.Index(j).Int(), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for _, i := range fields.booleans {
		fillBindForBool(bind, source.Field(i).Bool(), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.booleansArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForBool(bind, f.Index(j).Bool(), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.floats {
		err := fillBindForFloat(bind, source.Field(i), prefix+fields.fields[i].Name,
			fields.floatsPrecision[k], fields.floatsSize[k], fields.floatsDecimalSize[k], fields.floatsUnsigned[k])
		if err != nil {
			return err
		}
	}
	for k, i := range fields.floatsArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForFloat(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1),
				fields.floatsPrecisionArray[k], fields.floatsSizeArray[k], fields.floatsDecimalSizeArray[k], fields.floatsUnsignedArray[k])
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.times {
		err := fillBindForTime(bind, source.Field(i), prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for _, i := range fields.timesArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForTime(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.dates {
		err := fillBindForDate(bind, source.Field(i), prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for _, i := range fields.datesArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForDate(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for k, i := range fields.strings {
		err := fillBindForString(bind, source.Field(i), prefix+fields.fields[i].Name, fields.stringMaxLengths[k], fields.stringsRequired[k])
		if err != nil {
			return err
		}
	}
	for k, i := range fields.stringsArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForString(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1), fields.stringMaxLengths[k], fields.stringsRequired[k])
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.uIntegersNullable {
		fillBindForUIntegersNullable(bind, source.Field(i), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.uIntegersNullableArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForUIntegersNullable(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for _, i := range fields.integersNullable {
		fillBindForIntegersNullable(bind, source.Field(i), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.integersNullableArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForIntegersNullable(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.stringsEnums {
		err := fillBindForEnums(bind, source.Field(i), fields.enums[k], prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for k, i := range fields.stringsEnumsArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForEnums(bind, f.Index(j), fields.enumsArray[k], prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.bytes {
		fillBindForBytes(bind, source.Field(i), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.bytesArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForBytes(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.sliceStringsSets {
		err := fillBindForSets(bind, source.Field(i), fields.sets[k], prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for k, i := range fields.sliceStringsSetsArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForSets(bind, f.Index(j), fields.setsArray[k], prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.booleansNullable {
		fillBindForBooleansNullable(bind, source.Field(i), prefix+fields.fields[i].Name)
	}
	for _, i := range fields.booleansNullableArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindForBooleansNullable(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.floatsNullable {
		err := fillBindForFloatsNullable(bind, source.Field(i), prefix+fields.fields[i].Name, fields.floatsNullableUnsigned[k],
			fields.floatsNullablePrecision[k], fields.floatsNullableSize[k], fields.floatsNullableDecimalSize[k])
		if err != nil {
			return err
		}
	}
	for k, i := range fields.floatsNullableArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForFloatsNullable(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1), fields.floatsNullableUnsignedArray[k],
				fields.floatsNullablePrecisionArray[k], fields.floatsNullableSizeArray[k], fields.floatsNullableDecimalSizeArray[k])
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.timesNullable {
		err := fillBindForTimesNullable(bind, source.Field(i), prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for _, i := range fields.timesNullableArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForTimesNullable(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.datesNullable {
		err := fillBindForDatesNullable(bind, source.Field(i), prefix+fields.fields[i].Name)
		if err != nil {
			return err
		}
	}
	for _, i := range fields.datesNullableArray {
		f := source.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindForDatesNullable(bind, f.Index(j), prefix+fields.fields[i].Name+"_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for j, i := range fields.structs {
		sub := fields.structsFields[j]
		err := fillBindFromOneSource(c, bind, source.Field(i), sub, prefix+sub.prefix)
		if err != nil {
			return err
		}
	}
	for j, i := range fields.structsArray {
		f := source.Field(i)
		sub := fields.structsFieldsArray[j]
		for k := 0; k < fields.arrays[i]; k++ {
			err := fillBindFromOneSource(c, bind, f.Index(k), sub, prefix+sub.prefix+"_"+strconv.Itoa(k+1)+"_")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func fillBindFromTwoSources(c Context, bind, oldBind Bind, source, before reflect.Value, fields *tableFields, prefix string) error {
	for _, i := range fields.uIntegers {
		fillBindsForUint(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.uIntegersArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForUint(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.references {
		f1 := source.Field(i)
		f2 := before.Field(i)
		err := fillBindsForReference(f1, f2, bind, oldBind, fields, i, fields.referencesRequired[k], prefix, "")
		if err != nil {
			return err
		}
	}
	for k, i := range fields.referencesArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForReference(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, fields.referencesRequiredArray[k], prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.integers {
		fillBindsForInt(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.integersArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForInt(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for _, i := range fields.booleans {
		fillBindsForBool(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.booleansArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForBool(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.floats {
		precision := fields.floatsPrecision[k]
		unsigned := fields.floatsUnsigned[k]
		decimalSize := fields.floatsDecimalSize[k]
		floatSize := fields.floatsSize[k]
		err := fillBindsForFloat(source.Field(i), before.Field(i), bind, oldBind, fields,
			i, precision, decimalSize, floatSize, unsigned, prefix, "")
		if err != nil {
			return err
		}
	}
	for k, i := range fields.floatsArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		precision := fields.floatsPrecisionArray[k]
		unsigned := fields.floatsUnsignedArray[k]
		decimalSize := fields.floatsDecimalSizeArray[k]
		floatSize := fields.floatsSizeArray[k]
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForFloat(f1.Index(j), f2.Index(j), bind, oldBind, fields,
				i, precision, decimalSize, floatSize, unsigned, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.times {
		err := fillBindsForTime(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
		if err != nil {
			return err
		}
	}
	for _, i := range fields.timesArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForTime(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.dates {
		err := fillBindsForDate(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
		if err != nil {
			return err
		}
	}
	for _, i := range fields.datesArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForDate(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for k, i := range fields.strings {
		err := fillBindsForString(source.Field(i), before.Field(i), bind, oldBind, fields, i,
			fields.stringMaxLengths[k], fields.stringsRequired[k], prefix, "")
		if err != nil {
			return err
		}
	}
	for k, i := range fields.stringsArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		maxLength := fields.stringMaxLengthsArray[k]
		isRequired := fields.stringsRequiredArray[k]
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForString(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, maxLength, isRequired, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.uIntegersNullable {
		fillBindsForUIntegersPointers(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.uIntegersNullableArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForUIntegersPointers(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for _, i := range fields.integersNullable {
		fillBindsForIntegersPointers(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.integersNullableArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForIntegersPointers(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.stringsEnums {
		err := fillBindsForEnum(source.Field(i), before.Field(i), bind, oldBind, fields, i, fields.enums[k], prefix, "")
		if err != nil {
			return err
		}
	}
	for k, i := range fields.stringsEnumsArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForEnum(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, fields.enumsArray[k], prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.bytes {
		fillBindsForBytes(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.bytesArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForBytes(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.sliceStringsSets {
		err := fillBindsForSet(source.Field(i), before.Field(i), bind, oldBind, fields, i, fields.sets[k], prefix, "")
		if err != nil {
			return err
		}
	}
	for k, i := range fields.sliceStringsSetsArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForSet(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, fields.setsArray[k], prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.booleansNullable {
		fillBindsForBoolNullable(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
	}
	for _, i := range fields.booleansNullableArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			fillBindsForBoolNullable(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
		}
	}
	for k, i := range fields.floatsNullable {
		precision := fields.floatsNullablePrecision[k]
		unsigned := fields.floatsNullableUnsigned[k]
		decimalSize := fields.floatsNullableDecimalSize[k]
		floatSize := fields.floatsNullableSize[k]
		err := fillBindsForFloatNullable(source.Field(i), before.Field(i), bind, oldBind, fields,
			i, precision, decimalSize, floatSize, unsigned, prefix, "")
		if err != nil {
			return err
		}
	}
	for k, i := range fields.floatsNullableArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		precision := fields.floatsNullablePrecisionArray[k]
		unsigned := fields.floatsNullableUnsignedArray[k]
		decimalSize := fields.floatsNullableDecimalSizeArray[k]
		floatSize := fields.floatsNullableSizeArray[k]
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForFloatNullable(f1.Index(j), f2.Index(j), bind, oldBind, fields,
				i, precision, decimalSize, floatSize, unsigned, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.timesNullable {
		err := fillBindsForTimeNullable(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
		if err != nil {
			return err
		}
	}
	for _, i := range fields.timesNullableArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForTimeNullable(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
			}
		}
	}
	for _, i := range fields.datesNullable {
		err := fillBindsForDateNullable(source.Field(i), before.Field(i), bind, oldBind, fields, i, prefix, "")
		if err != nil {
			return err
		}
	}
	for _, i := range fields.datesNullableArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindsForDateNullable(f1.Index(j), f2.Index(j), bind, oldBind, fields, i, prefix, "_"+strconv.Itoa(j+1))
			if err != nil {
				return err
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
	for k, i := range fields.structsArray {
		f1 := source.Field(i)
		f2 := before.Field(i)
		sub := fields.structsFieldsArray[k]
		for j := 0; j < fields.arrays[i]; j++ {
			err := fillBindFromTwoSources(c, bind, oldBind, f1.Index(j), f2.Index(j), sub, prefix+sub.prefix+"_"+strconv.Itoa(j+1)+"_")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func fillBindsForReference(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, isRequired bool, prefix, suffix string) error {
	v1 := uint64(0)
	v2 := uint64(0)
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		v1 = f1.Interface().(referenceInterface).getID()
		if isRequired && v1 == 0 {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "nil value not allowed"}
		}
	} else if isRequired {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "nil value not allowed"}
	}
	if !v2IsNil {
		v2 = f2.Interface().(referenceInterface).getID()
	}
	if v1IsNil != v2IsNil || v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
			oldBind[prefix+fields.fields[i].Name+suffix] = nullAsString
		} else {
			oldBind[prefix+fields.fields[i].Name+suffix] = strconv.FormatUint(v2, 10)
		}
	}
	return nil
}

func fillBindsForUint(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := f1.Uint()
	v2 := f2.Uint()
	if v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
			oldBind[prefix+fields.fields[i].Name+suffix] = zeroAsString
		} else {
			oldBind[prefix+fields.fields[i].Name+suffix] = strconv.FormatUint(v2, 10)
		}
	}
}

func fillBindsForInt(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := f1.Int()
	v2 := f2.Int()
	if v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
			oldBind[prefix+fields.fields[i].Name+suffix] = zeroAsString
		} else {
			oldBind[prefix+fields.fields[i].Name+suffix] = strconv.FormatInt(v2, 10)
		}
	}
}

func fillBindsForBool(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := f1.Bool()
	v2 := f2.Bool()
	if v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		return
	} else if fields.forcedOldBid[i] {
		if v2 {
			oldBind[prefix+fields.fields[i].Name] = "1"
		} else {
			oldBind[prefix+fields.fields[i].Name] = zeroAsString
		}
	}
}

func fillBindsForTime(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) error {
	v1 := f1.Interface().(time.Time)
	if v1.Location() != time.UTC {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "time must be in UTC location"}
	}
	v1Check := time.Date(v1.Year(), v1.Month(), v1.Day(), v1.Hour(), v1.Minute(), v1.Second(), 0, time.UTC)
	if v1 != v1Check {
		f1.Set(reflect.ValueOf(v1Check))
		v1 = v1Check
	}
	v2 := f2.Interface().(time.Time)
	if v1.Unix() != v2.Unix() {
		name := prefix + fields.fields[i].Name + suffix
		bind[name] = v1.Format(time.DateTime)
		oldBind[name] = v2.Format(time.DateTime)
	} else if fields.forcedOldBid[i] {
		oldBind[prefix+fields.fields[i].Name+suffix] = v2.Format(time.DateTime)
	}
	return nil
}

func fillBindsForTimeNullable(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) error {
	var v1 time.Time
	var v2 time.Time
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		v1 = f1.Elem().Interface().(time.Time)
		if v1.Location() != time.UTC {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "time must be in UTC location"}
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
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[name] = nullAsString
		} else {
			oldBind[name] = v2.Format(time.DateTime)
		}
	}
	return nil
}

func fillBindsForDateNullable(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) error {
	var v1 time.Time
	var v2 time.Time
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		v1 = f1.Elem().Interface().(time.Time)
		if v1.Location() != time.UTC {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "time must be in UTC location"}
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
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[name] = nullAsString
		} else {
			oldBind[name] = v2.Format(time.DateOnly)
		}
	}
	return nil
}

func fillBindsForDate(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) error {
	v1 := f1.Interface().(time.Time)
	if v1.Location() != time.UTC {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "time must be in UTC location"}
	}
	v1Check := time.Date(v1.Year(), v1.Month(), v1.Day(), 0, 0, 0, 0, time.UTC)
	if v1 != v1Check {
		f1.Set(reflect.ValueOf(v1Check))
		v1 = v1Check
	}
	v2 := f2.Interface().(time.Time)
	if v1.Unix() != v2.Unix() {
		name := prefix + fields.fields[i].Name + suffix
		bind[name] = v1.Format(time.DateOnly)
		oldBind[name] = v2.Format(time.DateOnly)
	} else if fields.forcedOldBid[i] {
		oldBind[prefix+fields.fields[i].Name+suffix] = v2.Format(time.DateOnly)
	}
	return nil
}

func fillBindsForString(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i, maxLength int, isRequired bool, prefix, suffix string) error {
	v1 := f1.String()
	if len(v1) > maxLength {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix,
			Message: fmt.Sprintf("text too long, max %d allowed", maxLength)}
	}
	if v1 == "" {
		if isRequired {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "empty string not allowed"}
		}
	}
	v2 := f2.String()
	if v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
		bind[name] = v1
		oldBind[name] = v2
		if isRequired {
			if v1 == "" {
				bind[name] = nullAsString
			}
			if v2 == "" {
				oldBind[name] = nullAsString
			}
		}
	} else if fields.forcedOldBid[i] {
		name := prefix + fields.fields[i].Name + suffix
		oldBind[name] = v2
		if v2 == "" {
			oldBind[name] = nullAsString
		}
	}
	return nil
}

func fillBindsForUIntegersPointers(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := uint64(0)
	v2 := uint64(0)
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		v1 = f1.Elem().Uint()
	}
	if !v2IsNil {
		v2 = f2.Elem().Uint()
	}
	if v1IsNil != v2IsNil || v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[prefix+fields.fields[i].Name] = nullAsString
		} else {
			oldBind[name] = strconv.FormatUint(v2, 10)
		}
	}
}

func fillBindsForIntegersPointers(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := int64(0)
	v2 := int64(0)
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		v1 = f1.Elem().Int()
	}
	if !v2IsNil {
		v2 = f2.Elem().Int()
	}
	if v1IsNil != v2IsNil || v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[name] = nullAsString
		} else {
			oldBind[name] = strconv.FormatInt(v2, 10)
		}
	}
}

func fillBindsForEnum(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, def *enumDefinition, prefix, suffix string) error {
	v1 := f1.String()
	v2 := f2.String()
	if v1 == "" {
		if def.required {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "empty value not allowed"}
		}
	} else if !slices.Contains(def.GetFields(), v1) {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: fmt.Sprintf("invalid value: %s", v1)}
	}
	if v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2 == "" && !def.required {
			oldBind[name] = nullAsString
		} else {
			oldBind[name] = v2
		}
	}
	return nil
}

func fillBindsForBytes(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := f1.Bytes()
	v2 := f2.Bytes()
	if !bytes.Equal(v1, v2) {
		name := prefix + fields.fields[i].Name + suffix
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
			oldBind[prefix+fields.fields[i].Name+suffix] = nullAsString
		} else {
			oldBind[prefix+fields.fields[i].Name+suffix] = string(v2)
		}
	}
}

func fillBindsForSet(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, def *enumDefinition, prefix, suffix string) error {
	if f1.IsNil() || f1.Len() == 0 {
		if def.required {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "empty value not allowed"}
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
				return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: fmt.Sprintf("invalid value: %s", v)}
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
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[name] = nullAsString
		} else {
			oldBind[name] = strings.Join(v2, ",")
		}
	}
	return nil
}

func fillBindsForFloatNullable(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i, precision, decimalSize, floatSize int, unsigned bool, prefix, suffix string) error {
	v1 := ""
	v2 := ""
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		f := f1.Elem().Float()
		if unsigned && f < 0 {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "negative value not allowed"}
		}
		roundV := roundFloat(f, precision)
		v1 = strconv.FormatFloat(roundV, 'f', precision, floatSize)
		if f != roundV {
			if floatSize == 32 {
				roundV32 := float32(roundV)
				f1.Set(reflect.ValueOf(&roundV32))
			} else {
				f1.Set(reflect.ValueOf(&roundV))
			}
		}
		if decimalSize != -1 && strings.Index(v1, ".") > decimalSize {
			return &BindError{Field: prefix + fields.fields[i].Name + suffix,
				Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
		}
	}
	if !v2IsNil {
		v2 = strconv.FormatFloat(f2.Elem().Float(), 'f', precision, floatSize)
	}
	if v1IsNil != v2IsNil || v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[name] = nullAsString
		} else {
			oldBind[name] = v2
		}
	}
	return nil
}

func fillBindsForBoolNullable(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i int, prefix, suffix string) {
	v1 := false
	v2 := false
	v1IsNil := f1.IsNil()
	v2IsNil := f2.IsNil()
	if !v1IsNil {
		v1 = f1.Elem().Bool()
	}
	if !v2IsNil {
		v2 = f2.Elem().Bool()
	}
	if v1IsNil != v2IsNil || v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		name := prefix + fields.fields[i].Name + suffix
		if v2IsNil {
			oldBind[name] = nullAsString
		} else if v2 {
			oldBind[name] = "1"
		} else {
			oldBind[name] = zeroAsString
		}
	}
}

func fillBindsForFloat(f1, f2 reflect.Value, bind, oldBind Bind, fields *tableFields, i, precision, decimalSize, floatSize int, unsigned bool, prefix, suffix string) error {
	v := f1.Float()
	if unsigned && v < 0 {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix, Message: "negative value not allowed"}
	}
	roundV := roundFloat(v, precision)
	v1 := strconv.FormatFloat(roundV, 'f', precision, floatSize)
	if v != roundV {
		f1.SetFloat(roundV)
	}
	if decimalSize != -1 && strings.Index(v1, ".") > decimalSize {
		return &BindError{Field: prefix + fields.fields[i].Name + suffix,
			Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
	}
	v2Float := f2.Float()
	v2 := strconv.FormatFloat(v2Float, 'f', precision, floatSize)
	if v1 != v2 {
		name := prefix + fields.fields[i].Name + suffix
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
		oldBind[prefix+fields.fields[i].Name+suffix] = v2
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

func convertBindToRedisValue(bind Bind, schema *entitySchema) []any {
	values := make([]any, len(bind)+1)
	values[0] = schema.structureHash
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
