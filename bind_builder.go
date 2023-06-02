package beeorm

import (
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

type bindBuilder struct {
	id         uint64
	orm        *ORM
	bind       Bind
	current    Bind
	sqlBind    map[string]string
	index      int
	buildSQL   bool
	hasCurrent bool
}

func newBindBuilder(id uint64, orm *ORM) *bindBuilder {
	b := &bindBuilder{
		id:       id,
		orm:      orm,
		buildSQL: !orm.delete,
		bind:     Bind{},
		index:    -1,
	}
	if orm.delete {
		b.sqlBind = nil
	} else {
		b.sqlBind = make(map[string]string)
	}
	if orm.delete || orm.tableSchema.hasLog || len(orm.tableSchema.cachedIndexesAll) > 0 {
		b.hasCurrent = true
		b.current = Bind{}
	}
	return b
}

func (b *bindBuilder) build(serializer *serializer, fields *tableFields, value reflect.Value, root bool) {
	if root {
		serializer.DeserializeUInteger()
	}
	b.buildRefs(serializer, fields, value)
	b.buildUIntegers(serializer, fields, value, root)
	b.buildIntegers(serializer, fields, value)
	b.buildBooleans(serializer, fields, value)
	b.buildFloats(serializer, fields, value)
	b.buildTimes(serializer, fields, value)
	b.buildDates(serializer, fields, value)
	b.buildFakeDelete(serializer, fields, value)
	b.buildStrings(serializer, fields, value)
	b.buildUIntegersNullable(serializer, fields, value)
	b.buildIntegersNullable(serializer, fields, value)
	b.buildEnums(serializer, fields, value)
	b.buildBytes(serializer, fields, value)
	b.buildSets(serializer, fields, value)
	b.buildBooleansNullable(serializer, fields, value)
	b.buildFloatsNullable(serializer, fields, value)
	b.buildTimesNullable(serializer, fields, value)
	b.buildDatesNullable(serializer, fields, value)
	b.buildJSONs(serializer, fields, value)
	b.buildRefsMany(serializer, fields, value)
	for k, i := range fields.structs {
		b.build(serializer, fields.structsFields[k], value.Field(i), false)
	}
}

func (b *bindBuilder) buildRefs(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.refs {
		b.index++
		f := value.Field(i)
		val := uint64(0)
		if !f.IsNil() {
			val = f.Elem().Field(1).Uint()
		}
		if b.orm.inDB {
			old := serializer.DeserializeUInteger()
			if b.hasCurrent {
				if old == 0 {
					b.current[b.orm.tableSchema.columnNames[b.index]] = nil
				} else {
					b.current[b.orm.tableSchema.columnNames[b.index]] = old
				}
			}
			if old == val {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if val == 0 {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = strconv.FormatUint(val, 10)
			}
		}
	}
}

func (b *bindBuilder) buildUIntegers(serializer *serializer, fields *tableFields, value reflect.Value, root bool) {
	for _, i := range fields.uintegers {
		b.index++
		val := value.Field(i).Uint()
		if i == 1 && root {
			serializer.DeserializeUInteger()
			continue
		}
		if b.orm.inDB {
			old := serializer.DeserializeUInteger()
			if b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = old
			}
			if old == val {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		b.bind[name] = val
		if b.buildSQL {
			b.sqlBind[name] = strconv.FormatUint(val, 10)
		}
	}
}

func (b *bindBuilder) buildIntegers(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.integers {
		b.index++
		val := value.Field(i).Int()
		if b.orm.inDB {
			old := serializer.DeserializeInteger()
			if b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = old
			}
			if old == val {
				continue
			}
		}

		name := b.orm.tableSchema.columnNames[b.index]
		b.bind[name] = val
		if b.buildSQL {
			b.sqlBind[name] = strconv.FormatInt(val, 10)
		}
	}
}

func (b *bindBuilder) buildBooleans(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.booleans {
		b.index++
		val := value.Field(i).Bool()
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = old
			}
			if old == val {
				continue
			}
		}

		name := b.orm.tableSchema.columnNames[b.index]
		b.bind[name] = val
		if b.buildSQL {
			if val {
				b.sqlBind[name] = "1"
			} else {
				b.sqlBind[name] = "0"
			}
		}
	}
}

func (b *bindBuilder) buildFloats(serializer *serializer, fields *tableFields, value reflect.Value) {
	for k, i := range fields.floats {
		b.index++
		val := value.Field(i).Float()
		if b.orm.inDB {
			old := serializer.DeserializeFloat()
			if b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = old
			}
			if math.Abs(val-old) < (1 / math.Pow10(fields.floatsPrecision[k])) {
				continue
			}
		}

		name := b.orm.tableSchema.columnNames[b.index]
		b.bind[name] = val
		if b.buildSQL {
			b.sqlBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
		}
	}
}

func (b *bindBuilder) buildTimes(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.times {
		b.index++
		f := value.Field(i)
		t := f.Interface().(time.Time)
		isZero := t.IsZero() || t.UTC().Unix() == -30610224000
		if b.orm.inDB {
			old := serializer.DeserializeInteger()
			if old == zeroDateSeconds {
				old = 0
			} else {
				old -= timeStampSeconds
			}
			if b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = time.Unix(old, 0).Format(timeFormat)
			}
			if (old == 0 && isZero) || (old == t.Unix()) {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		asString := t.Format(timeFormat)
		b.bind[name] = asString
		if b.buildSQL {
			b.sqlBind[name] = "'" + asString + "'"
		}
	}
}

func (b *bindBuilder) buildDates(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.dates {
		b.index++
		t := value.Field(i).Interface().(time.Time)
		isZero := t.IsZero() || t.UTC().Unix() == -30610224000
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		if b.orm.inDB {
			old := serializer.DeserializeInteger()
			if old == zeroDateSeconds {
				old = 0
			} else {
				old -= timeStampSeconds
			}
			if b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = time.Unix(old, 0).Format(dateformat)
			}
			if old == 0 && isZero || old == t.Unix() {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		asString := t.Format(dateformat)
		b.bind[name] = asString
		if b.buildSQL {
			b.sqlBind[name] = "'" + asString + "'"
		}
	}
}

func (b *bindBuilder) buildFakeDelete(serializer *serializer, fields *tableFields, value reflect.Value) {
	if fields.fakeDelete > 0 {
		b.index++
		val := value.Field(fields.fakeDelete).Bool()
		fakeID := uint64(0)
		if val {
			fakeID = b.id
		}
		if b.hasCurrent && b.orm.inDB {
			b.current[b.orm.tableSchema.columnNames[b.index]] = fakeID
		}
		add := true
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if b.hasCurrent {
				if old {
					b.current[b.orm.tableSchema.columnNames[b.index]] = b.id
				} else {
					b.current[b.orm.tableSchema.columnNames[b.index]] = uint64(0)
				}
			}
			if old == val {
				add = false
			}
		}
		if add {
			name := b.orm.tableSchema.columnNames[b.index]
			b.bind[name] = fakeID
			if b.buildSQL {
				b.sqlBind[name] = strconv.FormatUint(fakeID, 10)
			}
		}
	}
}

func (b *bindBuilder) buildStrings(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.strings {
		b.index++
		val := value.Field(i).String()
		name := b.orm.tableSchema.columnNames[b.index]
		if b.orm.inDB {
			old := serializer.DeserializeString()
			if b.hasCurrent {
				if old == "" {
					attributes := b.orm.tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						b.current[b.orm.tableSchema.columnNames[b.index]] = ""
					} else {
						b.current[b.orm.tableSchema.columnNames[b.index]] = nil
					}
				} else {
					b.current[b.orm.tableSchema.columnNames[b.index]] = old
				}
			}
			if old == val {
				continue
			}
		}
		if val != "" {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = escapeSQLString(val)
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.bind[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "''"
				}
			} else {
				b.bind[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}

func (b *bindBuilder) buildUIntegersNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.uintegersNullable {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := uint64(0)
		if !isNil {
			val = f.Elem().Uint()
		}
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if !old && b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = nil
			}
			if old {
				oldVal := serializer.DeserializeUInteger()
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = oldVal
				}
				if oldVal == val && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if isNil {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = strconv.FormatUint(val, 10)
			}
		}
	}
}

func (b *bindBuilder) buildIntegersNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.integersNullable {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := int64(0)
		if !isNil {
			val = f.Elem().Int()
		}
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if !old && b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = nil
			}
			if old {
				oldVal := serializer.DeserializeInteger()
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = oldVal
				}
				if oldVal == val && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if isNil {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = strconv.FormatInt(val, 10)
			}
		}
	}
}

func (b *bindBuilder) buildEnums(serializer *serializer, fields *tableFields, value reflect.Value) {
	k := 0
	for _, i := range fields.stringsEnums {
		b.index++
		val := value.Field(i).String()
		enum := fields.enums[k]
		name := b.orm.tableSchema.columnNames[b.index]
		k++
		if b.orm.inDB {
			old := serializer.DeserializeUInteger()
			if b.hasCurrent {
				if old == 0 {
					b.current[name] = nil
				} else {
					b.current[name] = enum.GetFields()[old-1]
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
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = "'" + val + "'"
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				if b.orm.inDB {
					panic(fmt.Errorf("empty enum value for %s", name))
				}
				b.bind[name] = enum.GetDefault()
				if b.buildSQL {
					b.sqlBind[name] = "'" + enum.GetDefault() + "'"
				}
			} else {
				b.bind[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}

func (b *bindBuilder) buildBytes(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.bytes {
		b.index++
		val := string(value.Field(i).Bytes())
		if b.orm.inDB {
			old := serializer.DeserializeString()
			if b.hasCurrent {
				if old != "" {
					b.current[b.orm.tableSchema.columnNames[b.index]] = val
				} else {
					b.current[b.orm.tableSchema.columnNames[b.index]] = nil
				}
			}
			if old == val {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if val != "" {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = escapeSQLString(val)
			}
		} else {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		}
	}
}

func (b *bindBuilder) buildSets(serializer *serializer, fields *tableFields, value reflect.Value) {
	k := 0
	for _, i := range fields.sliceStringsSets {
		b.index++
		val := value.Field(i).Interface().([]string)
		set := fields.sets[k]
		l := len(val)
		k++
		name := b.orm.tableSchema.columnNames[b.index]
		if b.orm.inDB {
			old := int(serializer.DeserializeUInteger())
			if b.hasCurrent {
				attributes := b.orm.tableSchema.tags[name]
				required, hasRequired := attributes["required"]
				if hasRequired && required == "true" {
					b.current[name] = ""
				} else {
					b.current[name] = nil
				}
			}
			if l == old {
				if l == 0 {
					continue
				}
				oldValues := make([]int, l)
				if b.hasCurrent {
					b.current[name] = ""
				}
				for j := 0; j < old; j++ {
					oldValues[j] = int(serializer.DeserializeUInteger())
					if b.hasCurrent {
						b.current[name] = b.current[name].(string) + "," + set.GetFields()[oldValues[j]-1]
					}
				}
				if b.hasCurrent {
					b.current[name] = b.current[name].(string)[1:]
				}
				valid := true
			MAIN:
				for _, v := range val {
					enumIndex := set.Index(v)
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
			} else {
				for j := 0; j < old; j++ {
					serializer.DeserializeUInteger()
				}
			}
		}
		if l > 0 {
			valAsString := strings.Join(val, ",")
			b.bind[name] = valAsString
			if b.buildSQL {
				b.sqlBind[name] = "'" + valAsString + "'"
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.bind[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "''"
				}
			} else {
				b.bind[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}

func (b *bindBuilder) buildBooleansNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.booleansNullable {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := false
		if !isNil {
			val = f.Elem().Bool()
		}
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if !old && b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = nil
			}
			if old {
				oldVal := serializer.DeserializeBool()
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = oldVal
				}
				if oldVal == val && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if isNil {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			b.bind[name] = val
			if b.buildSQL {
				if val {
					b.sqlBind[name] = "1"
				} else {
					b.sqlBind[name] = "0"
				}
			}
		}
	}
}

func (b *bindBuilder) buildFloatsNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	for k, i := range fields.floatsNullable {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		val := float64(0)
		if !isNil {
			val = f.Elem().Float()
		}
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if !old && b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = nil
			}
			if old {
				v := serializer.DeserializeFloat()
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = v
				}
				if !isNil && math.Abs(val-v) < (1/math.Pow10(fields.floatsNullablePrecision[k])) {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if isNil {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		}
	}
}

func (b *bindBuilder) buildTimesNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.timesNullable {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val *time.Time
		if !isNil {
			val = f.Interface().(*time.Time)
		}
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if !old && b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = nil
			}
			if old {
				oldVal := serializer.DeserializeInteger() - timeStampSeconds
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = time.Unix(oldVal, 0).Format(timeFormat)
				}
				if !isNil && val != nil && oldVal == val.Unix() {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if val == nil {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			asString := val.Format(timeFormat)
			b.bind[name] = asString
			if b.buildSQL {
				b.sqlBind[name] = "'" + asString + "'"
			}
		}
	}
}

func (b *bindBuilder) buildDatesNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.datesNullable {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val time.Time
		if !isNil {
			val = *f.Interface().(*time.Time)
			val = time.Date(val.Year(), val.Month(), val.Day(), 0, 0, 0, 0, val.Location())
		}
		if b.orm.inDB {
			old := serializer.DeserializeBool()
			if !old && b.hasCurrent {
				b.current[b.orm.tableSchema.columnNames[b.index]] = nil
			}
			if old {
				oldVal := serializer.DeserializeInteger() - timeStampSeconds
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = time.Unix(oldVal, 0).Format(dateformat)
				}
				if oldVal == val.Unix() && !isNil {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := b.orm.tableSchema.columnNames[b.index]
		if isNil {
			b.bind[name] = nil
			if b.buildSQL {
				b.sqlBind[name] = "NULL"
			}
		} else {
			asString := val.Format(dateformat)
			b.bind[name] = asString
			if b.buildSQL {
				b.sqlBind[name] = "'" + asString + "'"
			}
		}
	}
}

func (b *bindBuilder) buildJSONs(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.jsons {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val interface{}
		asString := ""
		encoded := false
		name := b.orm.tableSchema.columnNames[b.index]
		if !isNil {
			val = f.Interface()
		}
		if b.orm.inDB {
			old := serializer.DeserializeBytes()
			if len(old) == 0 {
				if b.hasCurrent {
					attributes := b.orm.tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						b.current[b.orm.tableSchema.columnNames[b.index]] = ""
					} else {
						b.current[b.orm.tableSchema.columnNames[b.index]] = nil
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
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = string(old)
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
			b.bind[name] = asString
			if b.buildSQL {
				b.sqlBind[name] = escapeSQLString(asString)
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.bind[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "''"
				}
			} else {
				b.bind[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}

func (b *bindBuilder) buildRefsMany(serializer *serializer, fields *tableFields, value reflect.Value) {
	for _, i := range fields.refsMany {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val string
		name := b.orm.tableSchema.columnNames[b.index]
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
		if b.orm.inDB {
			l := int(serializer.DeserializeUInteger())
			if l == 0 {
				if b.hasCurrent {
					attributes := b.orm.tableSchema.tags[name]
					required, hasRequired := attributes["required"]
					if hasRequired && required == "true" {
						b.current[b.orm.tableSchema.columnNames[b.index]] = ""
					} else {
						b.current[b.orm.tableSchema.columnNames[b.index]] = nil
					}
				}
				if val == "" {
					continue
				}
			} else if val != "" {
				old := "[" + strconv.FormatUint(serializer.DeserializeUInteger(), 10)
				for j := 1; j < l; j++ {
					old += "," + strconv.FormatUint(serializer.DeserializeUInteger(), 10)
				}
				old += "]"
				if b.hasCurrent {
					b.current[b.orm.tableSchema.columnNames[b.index]] = old
				}
				if old == val {
					continue
				}
			} else {
				for j := 0; j < l; j++ {
					serializer.DeserializeUInteger()
				}
			}
		}
		if val != "" {
			b.bind[name] = val
			if b.buildSQL {
				b.sqlBind[name] = "'" + val + "'"
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.bind[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "'[]'"
				}
			} else {
				b.bind[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}
