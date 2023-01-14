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

const NullBindValue = "NULL"

type EntitySQLFlush struct {
	Action            FlushType
	EntityName        string
	ID                uint64
	Old               Bind
	Update            Bind
	UpdateOnDuplicate Bind
	TempID            uintptr
	References        map[string]uintptr
	flushed           bool
	entity            Entity
}

type entityFlushBuilder struct {
	*EntitySQLFlush
	orm          *ORM
	index        int
	fillOld      bool
	forceFillOld bool
	fillNew      bool
}

func newEntitySQLFlushBuilder(orm *ORM) *entityFlushBuilder {
	action := Insert
	if orm.delete {
		action = Delete
	} else if orm.onDuplicateKeyUpdate != nil {
		action = InsertUpdate
	} else if orm.inDB {
		action = Update
	}
	schema := orm.tableSchema
	flushData := &EntitySQLFlush{}
	flushData.Action = action
	flushData.EntityName = schema.t.String()
	flushData.ID = orm.GetID()
	flushData.TempID = orm.value.Pointer()
	b := &entityFlushBuilder{
		EntitySQLFlush: flushData,
		orm:            orm,
		index:          -1,
	}
	b.fillOld = action == Update || action == Delete
	b.forceFillOld = action == Delete
	b.fillNew = !b.forceFillOld
	if b.fillNew || b.forceFillOld {
		b.Old = make(Bind)
	}
	return b
}

func (b *entityFlushBuilder) fill(serializer *serializer, fields *tableFields, value reflect.Value, root bool) {
	if root {
		serializer.DeserializeUInteger()
	}
	b.buildUIntegers(serializer, fields, value)
	b.buildRefs(serializer, fields, value)
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
	for k, i := range fields.structs {
		b.fill(serializer, fields.structsFields[k], value.Field(i), false)
	}
	if root && b.orm.onDuplicateKeyUpdate != nil {
		b.UpdateOnDuplicate = map[string]string{}
		for k, v := range b.orm.onDuplicateKeyUpdate {
			b.UpdateOnDuplicate[k] = v
		}
	}
}

type fieldDataProvider struct {
	fieldGetter          func(field reflect.Value) interface{}
	serializeGetter      func(s *serializer, field reflect.Value) interface{}
	bindSetter           func(val interface{}, deserialized bool) string
	bindCompare          func(old, new interface{}, key int, fields *tableFields) bool
	bindCompareAndSetter func(old, new interface{}, field reflect.Value) (bool, string, string)
}

func (b *entityFlushBuilder) build(serializer *serializer, fields *tableFields, value reflect.Value, indexes []int, provider fieldDataProvider) {
	for key, i := range indexes {
		b.index++
		f := value.Field(i)
		val := provider.fieldGetter(f)
		if b.fillOld {
			old := provider.serializeGetter(serializer, f)
			var same bool
			if provider.bindCompareAndSetter != nil {
				same, old, val = provider.bindCompareAndSetter(old, val, f)
			} else if provider.bindCompare != nil {
				same = provider.bindCompare(old, val, key, fields)
			} else {
				same = old == val
			}

			if b.forceFillOld || !same {
				if provider.bindCompareAndSetter != nil {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = old.(string)
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
			if b.Update == nil {
				b.Update = Bind{}
			}
			if b.fillOld && provider.bindCompareAndSetter != nil {
				b.Update[name] = val.(string)
			} else {
				b.Update[name] = provider.bindSetter(val, false)
			}
		}
	}
}

func (b *entityFlushBuilder) buildNullable(serializer *serializer, fields *tableFields, value reflect.Value, indexes []int, provider fieldDataProvider) {
	for key, i := range indexes {
		b.index++
		f := value.Field(i)
		isNil := f.IsNil()
		var val interface{}
		if !isNil {
			val = provider.fieldGetter(f.Elem())
		}
		if b.fillOld {
			var oldVal interface{}
			oldIsNil := !serializer.DeserializeBool()
			if !oldIsNil {
				oldVal = provider.serializeGetter(serializer, f)
			}
			same := oldIsNil == isNil
			if same && !isNil {
				if provider.bindCompare != nil {
					same = provider.bindCompare(oldVal, val, key, fields)
				} else {
					same = oldVal == val
				}
			}
			if b.forceFillOld || !same {
				if oldIsNil {
					b.Old[b.orm.tableSchema.columnNames[b.index]] = NullBindValue
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
				b.Update[name] = NullBindValue
			} else {
				b.Update[name] = provider.bindSetter(val, false)
			}
		}
	}
}

func serializeGetterUint(s *serializer, _ reflect.Value) interface{} {
	return s.DeserializeUInteger()
}

func (b *entityFlushBuilder) buildRefs(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		s,
		fields,
		value,
		fields.refs,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				if field.IsNil() {
					return uint64(0)
				}
				return field.Elem().Field(1).Uint()
			},
			serializeGetter: serializeGetterUint,
			bindSetter: func(val interface{}, _ bool) string {
				id := val.(uint64)
				if id == 0 {
					return NullBindValue
				}
				return strconv.FormatUint(id, 10)
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

func (b *entityFlushBuilder) buildUIntegers(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.uintegers, uIntFieldDataProvider)
}

var intFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Int()
	},
	serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
		return s.DeserializeInteger()
	},
	bindSetter: func(val interface{}, _ bool) string {
		return strconv.FormatInt(val.(int64), 10)
	},
}

func (b *entityFlushBuilder) buildIntegers(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.integers, intFieldDataProvider)
}

var boolFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Bool()
	},
	serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
		return s.DeserializeBool()
	},
	bindSetter: func(val interface{}, _ bool) string {
		if val.(bool) {
			return "1"
		}
		return "0"
	},
}

func (b *entityFlushBuilder) buildBooleans(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.booleans, boolFieldDataProvider)
}

var floatFieldDataProvider = fieldDataProvider{
	fieldGetter: func(field reflect.Value) interface{} {
		return field.Float()
	},
	serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
		return s.DeserializeFloat()
	},
	bindSetter: func(val interface{}, _ bool) string {
		return strconv.FormatFloat(val.(float64), 'f', -1, 64)
	},
	bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
		return math.Abs(new.(float64)-old.(float64)) < (1 / math.Pow10(fields.floatsPrecision[key]))
	},
}

func (b *entityFlushBuilder) buildFloats(s *serializer, fields *tableFields, value reflect.Value) {
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
	serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
		val := s.DeserializeInteger()
		if val == zeroDateSeconds {
			val = 0
		} else {
			val -= timeStampSeconds
		}
		return val
	},
	bindSetter: dateTimeBindSetter(timeFormat),
	bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
		return (old == int64(0) && new.(time.Time).IsZero()) || (old == new.(time.Time).Unix())
	},
}

func (b *entityFlushBuilder) buildTimes(s *serializer, fields *tableFields, value reflect.Value) {
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

func (b *entityFlushBuilder) buildDates(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.dates, dateFieldDataProvider)
}

func (b *entityFlushBuilder) buildFakeDelete(s *serializer, fields *tableFields, value reflect.Value) {
	if fields.fakeDelete == 0 {
		return
	}
	b.build(
		s,
		fields,
		value,
		[]int{fields.fakeDelete},
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				return field.Bool()
			},
			serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
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

func (b *entityFlushBuilder) buildStrings(s *serializer, fields *tableFields, value reflect.Value) {
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
			serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
				return s.DeserializeString()
			},
			bindSetter: func(val interface{}, _ bool) string {
				str := val.(string)
				if str == "" && b.orm.tableSchema.GetTagBool(name, "required") {
					return NullBindValue
				}
				return str
			},
		})
}

func (b *entityFlushBuilder) buildUIntegersNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.uintegersNullable, uIntFieldDataProvider)
}

func (b *entityFlushBuilder) buildIntegersNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.integersNullable, intFieldDataProvider)
}

func (b *entityFlushBuilder) buildEnums(s *serializer, fields *tableFields, value reflect.Value) {
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
						return NullBindValue
					}
					return fields.enums[k].GetFields()[i-1]
				}
				str := val.(string)
				if str == "" {
					if b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
						return fields.enums[k].GetDefault()
					}
					return NullBindValue
				}
				return str
			},
			bindCompare: func(old, new interface{}, _ int, _ *tableFields) bool {
				return old == uint64(fields.enums[k].Index(new.(string)))
			},
		})
}

func (b *entityFlushBuilder) buildBytes(s *serializer, fields *tableFields, value reflect.Value) {
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
			serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
				return s.DeserializeString()
			},
			bindSetter: func(val interface{}, _ bool) string {
				str := val.(string)
				if str == "" {
					return NullBindValue
				}
				return ""
			},
		})
}

func (b *entityFlushBuilder) buildSets(s *serializer, fields *tableFields, value reflect.Value) {
	k := -1
	b.build(
		s,
		fields,
		value,
		fields.sliceStringsSets,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				k++
				if field.IsNil() {
					return nil
				}
				val := field.Interface().([]string)
				if len(val) == 0 {
					return nil
				}
				return val
			},
			serializeGetter: func(s *serializer, _ reflect.Value) interface{} {
				l := int(s.DeserializeUInteger())
				if l == 0 {
					return nil
				}
				res := make([]int, l)
				for j := 0; j < l; j++ {
					res[j] = int(s.DeserializeUInteger())
				}
				return res
			},
			bindSetter: func(val interface{}, deserialized bool) string {
				if val == nil {
					if b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
						return ""
					}
					return NullBindValue
				}
				if deserialized {
					ids := val.([]int)
					values := make([]string, len(ids))
					for i, id := range ids {
						values[i] = fields.sets[k].GetFields()[id-1]
					}
					return strings.Join(values, ",")
				}
				return strings.Join(val.([]string), ",")
			},
			bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
				oldIsNil := old == nil
				newIsNil := new == nil
				if oldIsNil != newIsNil {
					return false
				} else if oldIsNil {
					return true
				}
				oldSlice := old.([]int)
				newSlice := new.([]string)
				if len(oldSlice) != len(newSlice) {
					return false
				}
			MAIN:
				for _, checkOld := range oldSlice {
					for _, checkNew := range newSlice {
						if fields.sets[k].GetFields()[checkOld-1] == checkNew {
							continue MAIN
						}
					}
					return false
				}
				return true
			},
		})
}

func (b *entityFlushBuilder) buildBooleansNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.booleansNullable, boolFieldDataProvider)
}

func (b *entityFlushBuilder) buildFloatsNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.floatsNullable, floatFieldDataProvider)
}

func (b *entityFlushBuilder) buildTimesNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.timesNullable, dateTimeFieldDataProvider)
}

func (b *entityFlushBuilder) buildDatesNullable(s *serializer, fields *tableFields, value reflect.Value) {
	b.buildNullable(s, fields, value, fields.datesNullable, dateFieldDataProvider)
}

func (b *entityFlushBuilder) bindSetterForJSON(val interface{}, deserialized bool) string {
	if val == nil {
		if b.orm.tableSchema.GetTagBool(b.orm.tableSchema.columnNames[b.index], "required") {
			return ""
		}
		return NullBindValue
	}
	if deserialized {
		return string(val.([]byte))
	}
	v, err := jsoniter.ConfigFastest.MarshalToString(val)
	checkError(err)
	return v
}

func (b *entityFlushBuilder) buildJSONs(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(
		s,
		fields,
		value,
		fields.jsons,
		fieldDataProvider{
			fieldGetter: func(field reflect.Value) interface{} {
				return field.Interface()
			},
			serializeGetter: func(s *serializer, field reflect.Value) interface{} {
				v := s.DeserializeBytes()
				if v == nil {
					return nil
				}
				return v
			},
			bindSetter: b.bindSetterForJSON,
			bindCompareAndSetter: func(old, new interface{}, field reflect.Value) (bool, string, string) {
				oldIsNil := old == nil
				newIsNil := new == nil
				if oldIsNil != newIsNil {
					return false, b.bindSetterForJSON(old, true), b.bindSetterForJSON(new, false)
				} else if oldIsNil {
					return true, NullBindValue, NullBindValue
				}
				oldInstance := reflect.New(field.Type()).Elem().Interface()
				err := jsoniter.ConfigFastest.Unmarshal(old.([]byte), &oldInstance)
				checkError(err)
				newJSON, err := jsoniter.ConfigFastest.Marshal(new)
				checkError(err)
				newInstance := reflect.New(field.Type()).Elem().Interface()
				err = jsoniter.ConfigFastest.Unmarshal(newJSON, &newInstance)
				checkError(err)
				return cmp.Equal(oldInstance, newInstance), string(old.([]byte)), string(newJSON)
			},
		})
}
