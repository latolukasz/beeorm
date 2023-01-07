package beeorm

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"

	jsoniter "github.com/json-iterator/go"
)

type EntitySQLFlushData struct {
	Action            FlushType
	EntityName        string
	ID                uint64
	Old               BindSQL
	Update            BindSQL
	UpdateOnDuplicate BindSQL
}

type entityFlushDataBuilder struct {
	*EntitySQLFlushData
	orm          *ORM
	index        int
	fillOld      bool
	forceFillOld bool
	fillNew      bool
}

func newEntitySQLFlushDataBuilder(orm *ORM) *entityFlushDataBuilder {
	action := Insert
	if orm.delete {
		action = Delete
	} else if orm.onDuplicateKeyUpdate != nil {
		action = InsertUpdate
	} else if orm.inDB {
		action = Update
	}
	schema := orm.tableSchema
	flushData := &EntitySQLFlushData{}
	flushData.Action = action
	flushData.EntityName = schema.t.String()
	flushData.ID = orm.GetID()
	b := &entityFlushDataBuilder{
		EntitySQLFlushData: flushData,
		orm:                orm,
		index:              -1,
	}
	b.fillOld = action == Update || action == Delete
	b.forceFillOld = action == Delete
	b.fillNew = !b.forceFillOld
	return b
}

func (b *entityFlushDataBuilder) fill(serializer *serializer, fields *tableFields, value reflect.Value, root bool) {
	if root {
		serializer.DeserializeUInteger()
	}
	b.buildRefs(serializer, fields, value)
	b.buildUIntegers(serializer, fields, value)
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
		b.fill(serializer, fields.structsFields[k], value.Field(i), false)
	}
	if root && b.orm.onDuplicateKeyUpdate != nil {
		b.UpdateOnDuplicate = map[string]string{}
		for k, v := range b.orm.onDuplicateKeyUpdate {
			b.UpdateOnDuplicate[k] = escapeSQLValue(v)
		}
	}
}

type fieldGetter func(field reflect.Value) interface{}
type serializeGetter func() interface{}
type bindSetter func(val interface{}, deserialized bool) string
type bindCompare func(old, new interface{}, key int) bool

func (b *entityFlushDataBuilder) build(
	value reflect.Value,
	indexes []int,
	fGetter fieldGetter,
	sGetter serializeGetter,
	bSetter bindSetter,
	bCompare bindCompare) {
	for key, i := range indexes {
		b.index++
		f := value.Field(i)
		var val interface{}
		if !f.IsNil() {
			val = fGetter(f.Elem())
		}
		if b.fillOld {
			old := sGetter()
			same := bCompare(old, value, key)
			if b.forceFillOld || !same {
				if old == 0 {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = "NULL"
				} else {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = bSetter(old, true)
				}
			}
			if same {
				continue
			}
		}
		if b.fillNew {
			name := b.orm.tableSchema.columnNames[b.index]
			b.Update[name] = bSetter(val, false)
		}
	}
}

func (b *entityFlushDataBuilder) buildNullable(
	serializer *serializer,
	value reflect.Value,
	indexes []int,
	fGetter fieldGetter,
	sGetter serializeGetter,
	bSetter bindSetter,
	bCompare bindCompare) {
	for key, i := range indexes {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val interface{}
		if !isNil {
			val = fGetter(f.Elem())
		}
		if b.fillOld {
			old := serializer.DeserializeBool()
			var oldVal interface{}
			same := old == isNil
			if same && !isNil {
				oldVal = sGetter()
				same = bCompare(oldVal, val, key)
			}
			if b.forceFillOld || !same {
				if old {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = "NULL"
				} else {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = bSetter(oldVal, true)
				}
			}
			if same {
				continue
			}
		}
		if b.fillNew {
			name := b.orm.tableSchema.columnNames[b.index]
			if isNil {
				b.Update[name] = "NULL"
			} else {
				b.Update[name] = bSetter(val, false)
			}
		}
	}
}

func (b *entityFlushDataBuilder) buildRefs(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.uintegersNullable,
		func(field reflect.Value) interface{} {
			return field.Field(1).Uint()
		},
		func() interface{} {
			return serializer.DeserializeUInteger()
		},
		func(val interface{}, _ bool) string {
			if val == 0 {
				return "NULL"
			}
			return strconv.FormatUint(val.(uint64), 10)
		},
		func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildUIntegers(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.uintegers,
		func(field reflect.Value) interface{} {
			return field.Uint()
		},
		func() interface{} {
			return serializer.DeserializeUInteger()
		},
		func(val interface{}, _ bool) string {
			return strconv.FormatUint(val.(uint64), 10)
		},
		func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildIntegers(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.integers,
		func(field reflect.Value) interface{} {
			return field.Int()
		},
		func() interface{} {
			return serializer.DeserializeInteger()
		},
		func(val interface{}, _ bool) string {
			return strconv.FormatInt(val.(int64), 10)
		},
		func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildBooleans(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.booleans,
		func(field reflect.Value) interface{} {
			return field.Bool()
		},
		func() interface{} {
			return serializer.DeserializeBool()
		},
		func(val interface{}, _ bool) string {
			if val.(bool) {
				return "1"
			}
			return "0"
		},
		func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildFloats(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.floats,
		func(field reflect.Value) interface{} {
			return field.Float()
		},
		func() interface{} {
			return serializer.DeserializeFloat()
		},
		func(val interface{}, _ bool) string {
			return strconv.FormatFloat(val.(float64), 'f', -1, 64)
		},
		func(old, new interface{}, key int) bool {
			return math.Abs(new.(float64)-old.(float64)) < (1 / math.Pow10(fields.floatsPrecision[key]))
		})
}

func (b *entityFlushDataBuilder) buildTimes(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.times,
		func(field reflect.Value) interface{} {
			return field.Interface()
		},
		func() interface{} {
			return serializer.DeserializeInteger()
		},
		func(val interface{}, deserialized bool) string {
			if deserialized {
				t := val.(int64)
				if t == zeroDateSeconds {
					t = 0
				} else {
					t -= timeStampSeconds
				}
				return time.Unix(t, 0).Format(timeFormat)
			}
			return val.(time.Time).Format(timeFormat)
		},
		func(old, new interface{}, _ int) bool {
			return (old == 0 && new.(time.Time).IsZero()) || (old == new.(time.Time).Unix())
		})
}

func (b *entityFlushDataBuilder) buildDates(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.dates,
		func(field reflect.Value) interface{} {
			t := field.Interface().(time.Time)
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		},
		func() interface{} {
			return serializer.DeserializeInteger()
		},
		func(val interface{}, deserialized bool) string {
			if deserialized {
				t := val.(int64)
				if t == zeroDateSeconds {
					t = 0
				} else {
					t -= timeStampSeconds
				}
				return time.Unix(t, 0).Format(dateformat)
			}
			return val.(time.Time).Format(dateformat)
		},
		func(old, new interface{}, _ int) bool {
			return (old == 0 && new.(time.Time).IsZero()) || (old == new.(time.Time).Unix())
		})
}

func (b *entityFlushDataBuilder) buildFakeDelete(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		[]int{fields.fakeDelete},
		func(field reflect.Value) interface{} {
			return field.Bool()
		},
		func() interface{} {
			return serializer.DeserializeBool()
		},
		func(val interface{}, deserialized bool) string {
			if val.(bool) {
				return strconv.FormatUint(b.ID, 10)
			}
			return "0"
		},
		func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildStrings(serializer *serializer, fields *tableFields, value reflect.Value) {
	name := b.orm.tableSchema.columnNames[b.index]
	b.build(
		value,
		fields.strings,
		func(field reflect.Value) interface{} {
			return field.String()
		},
		func() interface{} {
			return serializer.DeserializeString()
		},
		func(val interface{}, deserialized bool) string {
			s := val.(string)
			if s == "" && b.orm.tableSchema.GetTagBool(name, "required") {
				return "NULL"
			}
			return s

		},
		func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildUIntegersNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(serializer,
		value,
		fields.uintegersNullable,
		func(field reflect.Value) interface{} {
			return field.Uint()
		},
		func() interface{} {
			return serializer.DeserializeUInteger()
		},
		func(val interface{}, _ bool) string {
			return strconv.FormatUint(val.(uint64), 10)
		}, func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildIntegersNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(serializer,
		value,
		fields.integersNullable,
		func(field reflect.Value) interface{} {
			return field.Int()
		},
		func() interface{} {
			return serializer.DeserializeInteger()
		},
		func(val interface{}, _ bool) string {
			return strconv.FormatInt(val.(int64), 10)
		}, func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildEnums(serializer *serializer, fields *tableFields, value reflect.Value) {
	k := -1
	b.build(
		value,
		fields.stringsEnums,
		func(field reflect.Value) interface{} {
			k++
			return field.String()
		},
		func() interface{} {
			return serializer.DeserializeUInteger()
		},
		func(val interface{}, deserialized bool) string {
			if deserialized {
				i := val.(uint64)
				if i == 0 {
					return "NULL"
				}
				return fields.enums[k].GetFields()[i-1]
			}
			s := val.(string)
			if s == "" && b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
				return "NULL"
			}
			return s
		},
		func(old, new interface{}, _ int) bool {
			return old == uint64(fields.enums[k].Index(new.(string)))
		})
}

func (b *entityFlushDataBuilder) buildBytes(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		value,
		fields.bytes,
		func(field reflect.Value) interface{} {
			if field.IsZero() {
				return ""
			}
			return string(field.Bytes())
		},
		func() interface{} {
			return serializer.DeserializeString()
		},
		func(val interface{}, _ bool) string {
			s := val.(string)
			if s == "" {
				return "NULL"
			}
			return ""
		}, func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildSets(serializer *serializer, fields *tableFields, value reflect.Value) {
	k := 0
	for _, i := range fields.sliceStringsSets {
		b.index++
		val := value.Field(i).Interface().([]string)
		set := fields.sets[k]
		l := len(val)
		k++
		name := b.orm.tableSchema.columnNames[b.index]
		if b.fillOld {
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
			b.Update[name] = valAsString
			if b.buildSQL {
				b.sqlBind[name] = "'" + valAsString + "'"
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.Update[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "''"
				}
			} else {
				b.Update[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}

func (b *entityFlushDataBuilder) buildBooleansNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(serializer,
		value,
		fields.booleansNullable,
		func(field reflect.Value) interface{} {
			return field.Bool()
		},
		func() interface{} {
			return serializer.DeserializeBool()
		},
		func(val interface{}, _ bool) string {
			if val.(bool) {
				return "1"
			}
			return "0"
		}, func(old, new interface{}, _ int) bool {
			return old == new
		})
}

func (b *entityFlushDataBuilder) buildFloatsNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(serializer,
		value,
		fields.floatsNullable,
		func(field reflect.Value) interface{} {
			return field.Float()
		},
		func() interface{} {
			return serializer.DeserializeFloat()
		},
		func(val interface{}, _ bool) string {
			return strconv.FormatFloat(val.(float64), 'f', -1, 64)
		}, func(old, new interface{}, index int) bool {
			return math.Abs(new.(float64)-old.(float64)) < (1 / math.Pow10(fields.floatsNullablePrecision[index]))
		})
}

func (b *entityFlushDataBuilder) buildTimesNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(
		serializer,
		value,
		fields.times,
		func(field reflect.Value) interface{} {
			return field.Interface()
		},
		func() interface{} {
			return serializer.DeserializeInteger()
		},
		func(val interface{}, deserialized bool) string {
			if deserialized {
				t := val.(int64)
				if t == zeroDateSeconds {
					t = 0
				} else {
					t -= timeStampSeconds
				}
				return time.Unix(t, 0).Format(timeFormat)
			}
			return val.(time.Time).Format(timeFormat)
		},
		func(old, new interface{}, _ int) bool {
			return (old == 0 && new.(time.Time).IsZero()) || (old == new.(time.Time).Unix())
		})
}

func (b *entityFlushDataBuilder) buildDatesNullable(serializer *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(
		serializer,
		value,
		fields.dates,
		func(field reflect.Value) interface{} {
			t := field.Interface().(time.Time)
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		},
		func() interface{} {
			return serializer.DeserializeInteger()
		},
		func(val interface{}, deserialized bool) string {
			if deserialized {
				t := val.(int64)
				if t == zeroDateSeconds {
					t = 0
				} else {
					t -= timeStampSeconds
				}
				return time.Unix(t, 0).Format(dateformat)
			}
			return val.(time.Time).Format(dateformat)
		},
		func(old, new interface{}, _ int) bool {
			return (old == 0 && new.(time.Time).IsZero()) || (old == new.(time.Time).Unix())
		})
}

func (b *entityFlushDataBuilder) buildJSONs(serializer *serializer, fields *tableFields, value reflect.Value) {
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
		if b.fillOld {
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
			b.Update[name] = asString
			if b.buildSQL {
				b.sqlBind[name] = EscapeSQLString(asString)
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.Update[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "''"
				}
			} else {
				b.Update[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}

func (b *entityFlushDataBuilder) buildRefsMany(serializer *serializer, fields *tableFields, value reflect.Value) {
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
		if b.fillOld {
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
			b.Update[name] = val
			if b.buildSQL {
				b.sqlBind[name] = "'" + val + "'"
			}
		} else {
			attributes := b.orm.tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				b.Update[name] = ""
				if b.buildSQL {
					b.sqlBind[name] = "'[]'"
				}
			} else {
				b.Update[name] = nil
				if b.buildSQL {
					b.sqlBind[name] = "NULL"
				}
			}
		}
	}
}
