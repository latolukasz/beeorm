package beeorm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/google/go-cmp/cmp"

	jsoniter "github.com/json-iterator/go"
)

const NullBindValue = "NULL"

type entitySQLFlush struct {
	Action            FlushType
	Entity            string
	ID                uint64
	Old               Bind
	Update            Bind
	UpdateOnDuplicate Bind
	Meta              Bind
	TempID            uint64
	References        map[string]uint64
	flushed           bool
	skip              bool
	clearLocalCache   bool
	entity            Entity
}

type EventEntityFlushQueryExecuted interface {
	Type() FlushType
	EntityName() string
	EntityID() uint64
	Before() Bind
	After() Bind
	EngineMeta() Bind
}

func (e *entitySQLFlush) Type() FlushType {
	return e.Action
}

func (e *entitySQLFlush) EntityName() string {
	return e.Entity
}

func (e *entitySQLFlush) EntityID() uint64 {
	return e.ID
}

func (e *entitySQLFlush) Before() Bind {
	return e.Old
}

func (e *entitySQLFlush) After() Bind {
	return e.Update
}

func (e *entitySQLFlush) EngineMeta() Bind {
	return e.Meta
}

type entityFlushBuilder struct {
	*entitySQLFlush
	orm          *ORM
	index        int
	fillOld      bool
	forceFillOld bool
	fillNew      bool
}

func newEntitySQLFlushBuilder(orm *ORM, forceFillOld bool) *entityFlushBuilder {
	action := Insert
	if orm.delete {
		action = Delete
	} else if orm.onDuplicateKeyUpdate != nil {
		action = insertUpdate
	} else if orm.inDB {
		action = Update
		if !orm.IsLoaded() {
			panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", orm.elem.Type().String(), orm.GetID()))
		}
	}
	schema := orm.entitySchema
	flushData := &entitySQLFlush{}
	flushData.Action = action
	flushData.Entity = schema.t.String()
	flushData.ID = orm.GetID()
	flushData.TempID = uint64(orm.value.Pointer())
	b := &entityFlushBuilder{
		entitySQLFlush: flushData,
		orm:            orm,
		index:          -1,
	}
	b.fillOld = forceFillOld || action == Update || action == Delete
	b.forceFillOld = forceFillOld || action == Delete
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
	bindSetter           func(val interface{}, deserialized bool, field reflect.Value) string
	bindCompare          func(old, new interface{}, key int, fields *tableFields) bool
	bindCompareAndSetter func(old, new interface{}, field reflect.Value) (bool, string, string)
}

func (b *entityFlushBuilder) build(serializer *serializer, fields *tableFields, value reflect.Value, indexes []int, provider fieldDataProvider) {
	for key, i := range indexes {
		b.index++
		name := b.orm.entitySchema.columnNames[b.index]
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
			forceOld := b.forceFillOld
			if same && !forceOld {
				_, forceOld = b.orm.entitySchema.cachedIndexesTrackedFields[name]
			}
			if forceOld || !same {
				if provider.bindCompareAndSetter != nil {
					b.Old[name] = old.(string)
				} else {
					b.Old[name] = provider.bindSetter(old, true, f)
				}
			}
			if same {
				continue
			}
		}
		if b.fillNew {
			if b.Update == nil {
				b.Update = Bind{}
			}
			if b.fillOld && provider.bindCompareAndSetter != nil {
				b.Update[name] = val.(string)
			} else {
				b.Update[name] = provider.bindSetter(val, false, f)
			}
		}
	}
}

func (b *entityFlushBuilder) buildNullable(serializer *serializer, fields *tableFields, value reflect.Value, indexes []int, provider fieldDataProvider) {
	for key, i := range indexes {
		b.index++
		name := b.orm.entitySchema.columnNames[b.index]
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
			forceOld := b.forceFillOld
			if same && !forceOld {
				_, forceOld = b.orm.entitySchema.cachedIndexesTrackedFields[name]
			}
			if forceOld || !same {
				if oldIsNil {
					b.Old[name] = NullBindValue
				} else {
					b.Old[name] = provider.bindSetter(oldVal, true, f)
				}
			}
			if same {
				continue
			}
		}
		if b.fillNew {
			if isNil {
				b.Update[name] = NullBindValue
			} else {
				b.Update[name] = provider.bindSetter(val, false, f)
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
				return field.Interface().(Entity).GetID()
			},
			serializeGetter: serializeGetterUint,
			bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
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
	bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
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
	bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
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
	bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
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
	bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
		return strconv.FormatFloat(val.(float64), 'f', -1, 64)
	},
	bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
		return math.Abs(new.(float64)-old.(float64)) < (1 / math.Pow10(fields.floatsPrecision[key]))
	},
}

func (b *entityFlushBuilder) buildFloats(s *serializer, fields *tableFields, value reflect.Value) {
	b.build(s, fields, value, fields.floats, floatFieldDataProvider)
}

func dateTimeBindSetter(format string) func(val interface{}, deserialized bool, _ reflect.Value) string {
	return func(val interface{}, deserialized bool, _ reflect.Value) string {
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
	bindSetter: dateTimeBindSetter(TimeFormat),
	bindCompare: func(old, new interface{}, key int, fields *tableFields) bool {
		t := new.(time.Time)
		isZero := t.IsZero() || t.UTC().Unix() == -30610224000
		return (old == int64(0) && isZero) || (old == new.(time.Time).Unix())
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
	bindSetter:      dateTimeBindSetter(DateFormat),
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
			bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
				if val.(bool) {
					return strconv.FormatUint(b.ID, 10)
				}
				return "0"
			},
		},
	)
}

func (b *entityFlushBuilder) buildStrings(s *serializer, fields *tableFields, value reflect.Value) {
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
			bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
				str := val.(string)
				name := b.orm.entitySchema.columnNames[b.index]
				if str == "" && !b.orm.entitySchema.GetTagBool(name, "required") {
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
			bindSetter: func(val interface{}, deserialized bool, field reflect.Value) string {
				if deserialized {
					i := val.(uint64)
					if i == 0 {
						return NullBindValue
					}
					return fields.enums[k].GetFields()[i-1]
				}
				str := val.(string)
				if str == "" {
					name := b.orm.entitySchema.columnNames[b.index]
					if b.orm.entitySchema.GetTagBool(name, "required") {
						if b.orm.inDB {
							panic(fmt.Errorf("empty enum value for %s", name))
						}
						field.SetString(fields.enums[k].GetDefault())
						return fields.enums[k].GetDefault()
					}
					return NullBindValue
				}
				if !fields.enums[k].Has(str) {
					panic(errors.New("unknown enum value for " + b.orm.entitySchema.columnNames[b.index] + " - " + str))
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
			bindSetter: func(val interface{}, _ bool, _ reflect.Value) string {
				str := val.(string)
				if str == "" {
					return NullBindValue
				}
				return str
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
			bindSetter: func(val interface{}, deserialized bool, _ reflect.Value) string {
				if val == nil {
					if b.orm.entitySchema.GetTagBool(b.orm.entitySchema.columnNames[b.index], "required") {
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

func (b *entityFlushBuilder) bindSetterForJSON(val interface{}, deserialized bool, field reflect.Value) string {
	if val == nil || (!deserialized && field.IsNil()) {
		if b.orm.entitySchema.GetTagBool(b.orm.entitySchema.columnNames[b.index], "required") {
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
				newIsNil := field.IsNil()
				if oldIsNil != newIsNil {
					return false, b.bindSetterForJSON(old, true, field), b.bindSetterForJSON(new, false, field)
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
