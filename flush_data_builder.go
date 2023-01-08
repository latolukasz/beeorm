package beeorm

import (
	"math"
	"reflect"
	"sort"
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

type fieldDataProvider struct {
	fieldGetter     func(field reflect.Value) interface{}
	serializeGetter func(s *serializer) interface{}
	bindSetter      func(val interface{}, deserialized bool) string
	bindCompare     func(old, new interface{}, key int, fields *tableFields) bool
}

func (b *entityFlushDataBuilder) build(serializer *serializer, fields *tableFields, value reflect.Value, indexes []int, provider fieldDataProvider) {
	for key, i := range indexes {
		b.index++
		f := value.Field(i)
		var val interface{}
		if !f.IsNil() {
			val = provider.fieldGetter(f.Elem())
		}
		if b.fillOld {
			old := provider.serializeGetter(serializer)
			var same bool
			if provider.bindCompare != nil {
				same = provider.bindCompare(old, val, key, fields)
			} else {
				same = old == val
			}

			if b.forceFillOld || !same {
				if old == 0 {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = "NULL"
				} else {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = provider.bindSetter(old, true)
				}
			}
			if same {
				continue
			}
		}
		if b.fillNew {
			name := b.orm.tableSchema.columnNames[b.index]
			b.Update[name] = provider.bindSetter(val, false)
		}
	}
}

func (b *entityFlushDataBuilder) buildNullable(serializer *serializer, fields *tableFields, value reflect.Value, indexes []int, provider fieldDataProvider) {
	for key, i := range indexes {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val interface{}
		if !isNil {
			val = provider.fieldGetter(f.Elem())
		}
		if b.fillOld {
			old := serializer.DeserializeBool()
			var oldVal interface{}
			same := old == isNil
			if same && !isNil {
				oldVal = provider.serializeGetter(serializer)
				if provider.bindCompare != nil {
					same = provider.bindCompare(oldVal, val, key, fields)
				} else {
					same = oldVal == val
				}
			}
			if b.forceFillOld || !same {
				if old {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = "NULL"
				} else {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = provider.bindSetter(oldVal, true)
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
				b.Update[name] = provider.bindSetter(val, false)
			}
		}
	}
}

func serializeGetterUint(s *serializer) interface{} {
	return s.DeserializeUInteger()
}

func (b *entityFlushDataBuilder) buildRefs(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		s,
		fields,
		value,
		fields.uintegersNullable,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				return field.Field(1).Uint()
			},
			serializeGetter: serializeGetterUint,
			bindSetter: func(val interface{}, _ bool) string {
				if val == 0 {
					return "NULL"
				}
				return strconv.FormatUint(val.(uint64), 10)
			},
		},
	)
}

var uIntFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Uint()
	},
	serializeGetter: serializeGetterUint,
	bindSetter: func(val interface{}, _ bool) string {
		return strconv.FormatUint(val.(uint64), 10)
	},
}

func (b *entityFlushDataBuilder) buildUIntegers(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.uintegers, uIntFieldDataProvider)
}

var intFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Int()
	},
	serializeGetter: func(s *serializer) interface{} {
		return s.DeserializeInteger()
	},
	bindSetter: func(val interface{}, _ bool) string {
		return strconv.FormatInt(val.(int64), 10)
	},
}

func (b *entityFlushDataBuilder) buildIntegers(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.integers, intFieldDataProvider)
}

var boolFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Bool()
	},
	serializeGetter: func(s *serializer) interface{} {
		return s.DeserializeBool()
	},
	bindSetter: func(val interface{}, _ bool) string {
		if val.(bool) {
			return "1"
		}
		return "0"
	},
}

func (b *entityFlushDataBuilder) buildBooleans(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.booleans, boolFieldDataProvider)
}

var floatFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Float()
	},
	serializeGetter: func(s *serializer) interface{} {
		return s.DeserializeFloat()
	},
	bindSetter: func(val interface{}, _ bool) string {
		return strconv.FormatFloat(val.(float64), 'f', -1, 64)
	},
	bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
		return math.Abs(new.(float64)-old.(float64)) < (1 / math.Pow10(fields.floatsPrecision[key]))
	},
}

func (b *entityFlushDataBuilder) buildFloats(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.floats, floatFieldDataProvider)
}

func dateTimeBindSetter(format string) func(val interface{}, deserialized bool) string {
	return func(val interface{}, deserialized bool) string {
		if deserialized {
			t := val.(int64)
			if t == zeroDateSeconds {
				t = 0
			} else {
				t -= timeStampSeconds
			}
			return time.Unix(t, 0).Format(format)
		}
		return val.(time.Time).Format(format)
	}
}

var dateTimeFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Interface()
	},
	serializeGetter: func(s *serializer) interface{} {
		return s.DeserializeInteger()
	},
	bindSetter: dateTimeBindSetter(timeFormat),
	bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
		return (old == 0 && new.(time.Time).IsZero()) || (old == new.(time.Time).Unix())
	},
}

func (b *entityFlushDataBuilder) buildTimes(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.times, dateTimeFieldDataProvider)
}

var dateFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		t := field.Interface().(time.Time)
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	},
	serializeGetter: dateTimeFieldDataProvider.serializeGetter,
	bindSetter:      dateTimeBindSetter(dateformat),
	bindCompare:     dateTimeFieldDataProvider.bindCompare,
}

func (b *entityFlushDataBuilder) buildDates(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.dates, dateFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildFakeDelete(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		s,
		fields,
		value,
		[]int{fields.fakeDelete},
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				return field.Bool()
			},
			serializeGetter: func(s *serializer) interface{} {
				return s.DeserializeBool()
			},
			bindSetter: func(val interface{}, _ bool) string {
				if val.(bool) {
					return strconv.FormatUint(b.ID, 10)
				}
				return "0"
			},
		},
	)
}

func (b *entityFlushDataBuilder) buildStrings(s *serializer, fields *tableFields, value reflect.Value) {
	name := b.orm.tableSchema.columnNames[b.index]
	b.build(
		s,
		fields,
		value,
		fields.strings,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				return field.String()
			},
			serializeGetter: func(s *serializer) interface{} {
				return s.DeserializeString()
			},
			bindSetter: func(val interface{}, _ bool) string {
				str := val.(string)
				if str == "" && b.orm.tableSchema.GetTagBool(name, "required") {
					return "NULL"
				}
				return str
			},
		})
}

func (b *entityFlushDataBuilder) buildUIntegersNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.uintegersNullable, uIntFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildIntegersNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.integersNullable, intFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildEnums(s *serializer, fields *tableFields, value reflect.Value) {
	k := -1
	b.build(
		s,
		fields,
		value,
		fields.stringsEnums,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				k++
				return field.String()
			},
			serializeGetter: serializeGetterUint,
			bindSetter: func(val interface{}, deserialized bool) string {
				if deserialized {
					i := val.(uint64)
					if i == 0 {
						return "NULL"
					}
					return fields.enums[k].GetFields()[i-1]
				}
				s := val.(string)
				if s == "" && b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
					return fields.enums[k].GetDefault()
				}
				return s
			},
			bindCompare: func(old, new interface{}, _ int, _ *tableFields) bool {
				return old == uint64(fields.enums[k].Index(new.(string)))
			},
		})
}

func (b *entityFlushDataBuilder) buildBytes(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		s,
		fields,
		value,
		fields.bytes,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				if field.IsZero() {
					return ""
				}
				return string(field.Bytes())
			},
			serializeGetter: func(s *serializer) interface{} {
				return s.DeserializeString()
			},
			bindSetter: func(val interface{}, _ bool) string {
				str := val.(string)
				if str == "" {
					return "NULL"
				}
				return ""
			},
		})
}

func (b *entityFlushDataBuilder) buildSets(s *serializer, fields *tableFields, value reflect.Value) {
	k := -1
	b.build(
		s,
		fields,
		value,
		fields.sliceStringsSets,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				k++
				return field.Interface()
			},
			serializeGetter: serializeGetterUint,
			bindSetter: func(val interface{}, deserialized bool) string {
				if deserialized {
					i := int(val.(uint64))
					if i == 0 {
						if b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
							return ""
						}
						return "NULL"
					}
					values := make([]string, i)
					for j := 0; j < i; j++ {
						values[j] = fields.enums[k].GetFields()[s.DeserializeUInteger()-1]
					}
					return strings.Join(values, ",")
				}
				values := val.([]string)
				if len(values) == 0 {
					if b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
						return ""
					}
					return "NULL"
				}
				sort.Strings(values)
				return strings.Join(values, ",")
			},
		})
}

func (b *entityFlushDataBuilder) buildBooleansNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.booleansNullable, boolFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildFloatsNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.floatsNullable, floatFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildTimesNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.timesNullable, dateTimeFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildDatesNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.datesNullable, dateFieldDataProvider)
}

func (b *entityFlushDataBuilder) buildJSONs(s *serializer, fields *tableFields, value reflect.Value) {
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

func (b *entityFlushDataBuilder) buildRefsMany(s *serializer, fields *tableFields, value reflect.Value) {
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
