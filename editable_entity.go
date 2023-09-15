package beeorm

import (
	"reflect"
)

type FlushType int

const (
	Insert FlushType = iota
	Update
	Delete
)

type Meta map[string]string

type EntityFlush interface {
	FlushType() FlushType
	Schema() *entitySchema
	GetMetaData() Meta
}

type EntityFlushInsert interface {
	EntityFlush
	GetBind() Bind
}

type EntityFlushedEvent interface {
	FlushType() FlushType
	GetMetaData() Meta
}

func (m Meta) Get(key string) string {
	return m[key]
}

type writableEntityInterface[E Entity] interface {
	SetMetaData(key, value string)
	GetMetaData() Meta
}

type InsertableEntity[E Entity] interface {
	writableEntityInterface[E]
	TrackedEntity() E
	SetOnDuplicateKeyUpdate(bind Bind)
	GetBind() Bind
}

type RemovableEntity[E Entity] interface {
	writableEntityInterface[E]
}

type EditableEntity[E Entity] interface {
	writableEntityInterface[E]
	TrackedEntity() E
	SourceEntity() E
	GetBind() (new, old Bind)
}

type writableEntity[E Entity] struct {
	c      Context
	schema *entitySchema
	meta   Meta
}

func (w *writableEntity[E]) SetMetaData(key, value string) {
	if w.meta == nil {
		if value != "" {
			w.meta = Meta{key: value}
		}
	} else if value == "" {
		delete(w.meta, key)
	} else {
		w.meta[key] = value
	}
}

func (w *writableEntity[E]) Schema() *entitySchema {
	return w.schema
}

func (w *writableEntity[E]) GetMetaData() Meta {
	return w.meta
}

type insertableEntity[E Entity] struct {
	writableEntity[E]
	entity               E
	value                reflect.Value
	onDuplicateKeyUpdate Bind
}

func (m *insertableEntity[E]) SetOnDuplicateKeyUpdate(bind Bind) {
	m.onDuplicateKeyUpdate = bind
}

func (m *insertableEntity[E]) TrackedEntity() E {
	return m.entity
}

func (m *insertableEntity[E]) FlushType() FlushType {
	return Insert
}

func (e *editableEntity[E]) FlushType() FlushType {
	return Update
}

type removableEntity[E Entity] struct {
	writableEntity[E]
	source E
	delete bool
}

func (r *removableEntity[E]) FlushType() FlushType {
	return Delete
}

type editableEntity[E Entity] struct {
	writableEntity[E]
	entity E
	value  reflect.Value
	source E
}

func (e *editableEntity[E]) TrackedEntity() E {
	return e.entity
}

func (e *editableEntity[E]) SourceEntity() E {
	return e.source
}

func NewEntity[E Entity](c Context) InsertableEntity[E] {
	newEntity := &insertableEntity[E]{}
	newEntity.c = c
	schema := GetEntitySchema[E](c).(*entitySchema)
	newEntity.schema = schema
	value := reflect.New(schema.GetType().Elem())
	elem := value.Elem()
	initNewEntity(elem, schema.fields)
	newEntity.entity = value.Interface().(E)
	elem.Field(0).SetUint(schema.uuid())
	newEntity.value = value
	ci := c.(*contextImplementation)
	ci.trackedEntities = append(ci.trackedEntities, newEntity)
	return newEntity
}

func RemoveEntity[E Entity](c Context, source E) RemovableEntity[E] {
	toRemove := &removableEntity[E]{}
	toRemove.c = c
	toRemove.source = source
	return toRemove
}

func CloneForEdit[E Entity](c Context, source E) EditableEntity[E] {
	writable := Copy[E](c, source).(*editableEntity[E])
	writable.source = source
	ci := c.(*contextImplementation)
	ci.trackedEntities = append(ci.trackedEntities, writable)
	return writable
}

func Copy[E Entity](c Context, source E) EditableEntity[E] {
	cloned := *new(E)
	writable := &editableEntity[E]{}
	writable.c = c
	writable.entity = cloned
	writable.value = reflect.ValueOf(writable.entity)
	schema := GetEntitySchema[E](c)
	s := c.getSerializer()
	serializeEntity(schema, reflect.ValueOf(source), s)
	deserializeFromBinary(s, schema, writable.value)
	return writable
}

func initNewEntity(elem reflect.Value, fields *tableFields) {
	for k, i := range fields.stringsEnums {
		def := fields.enums[k]
		if def.required {
			elem.Field(i).SetString(def.defaultValue)
		}
	}
	for k, i := range fields.sliceStringsSets {
		def := fields.enums[k]
		if def.required {
			f := elem.Field(i)
			setValues := reflect.MakeSlice(f.Type(), 1, 1)
			setValues.Index(0).SetString(def.defaultValue)
			f.Set(setValues)
		}
	}
}
