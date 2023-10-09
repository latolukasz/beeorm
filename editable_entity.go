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

type EntityFlush interface {
	ID() uint64
	Schema() *entitySchema
	flushType() FlushType
}

type entityFlushInsert interface {
	EntityFlush
	getBind() (Bind, error)
	getEntity() any
	getValue() reflect.Value
}

type entityFlushDelete interface {
	EntityFlush
	getOldBind() (Bind, error)
}

type entityFlushUpdate interface {
	EntityFlush
	getBind() (new, old Bind, err error)
	getValue() reflect.Value
	getSourceValue() reflect.Value
	getEntity() any
}

type EntityFlushedEvent interface {
	FlushType() FlushType
}
type writableEntityInterface[E any] interface {
}

type InsertableEntity[E any] interface {
	writableEntityInterface[E]
	TrackedEntity() *E
	getBind() (Bind, error)
}

type RemovableEntity[E any] interface {
	writableEntityInterface[E]
	SourceEntity() *E
}

type EditableEntity[E any] interface {
	writableEntityInterface[E]
	TrackedEntity() *E
	SourceEntity() *E
	getBind() (new, old Bind, err error)
}

type writableEntity[E any] struct {
	c      Context
	schema *entitySchema
}

func (w *writableEntity[E]) Schema() *entitySchema {
	return w.schema
}

type insertableEntity[E any] struct {
	writableEntity[E]
	entity *E
	id     uint64
	value  reflect.Value
}

func (m *insertableEntity[E]) ID() uint64 {
	return m.id
}

func (m *insertableEntity[E]) TrackedEntity() *E {
	return m.entity
}

func (m *insertableEntity[E]) getEntity() any {
	return m.entity
}

func (m *insertableEntity[E]) flushType() FlushType {
	return Insert
}

func (m *insertableEntity[E]) getValue() reflect.Value {
	return m.value
}

func (e *editableEntity[E]) flushType() FlushType {
	return Update
}

func (e *editableEntity[E]) getValue() reflect.Value {
	return e.value
}

func (e *editableEntity[E]) getEntity() any {
	return e.entity
}

func (e *editableEntity[E]) getSourceValue() reflect.Value {
	return e.sourceValue
}

type removableEntity[E any] struct {
	writableEntity[E]
	id     uint64
	source *E
}

func (r *removableEntity[E]) flushType() FlushType {
	return Delete
}

func (r *removableEntity[E]) SourceEntity() *E {
	return r.source
}

type editableEntity[E any] struct {
	writableEntity[E]
	entity      *E
	id          uint64
	value       reflect.Value
	sourceValue reflect.Value
	source      *E
}

func (e *editableEntity[E]) ID() uint64 {
	return e.id
}

func (r *removableEntity[E]) ID() uint64 {
	return r.id
}

func (e *editableEntity[E]) TrackedEntity() *E {
	return e.entity
}

func (e *editableEntity[E]) SourceEntity() *E {
	return e.source
}

func NewEntity[E any](c Context) InsertableEntity[E] {
	newEntity := &insertableEntity[E]{}
	newEntity.c = c
	schema := getEntitySchema[E](c)
	newEntity.schema = schema
	value := reflect.New(schema.t)
	elem := value.Elem()
	initNewEntity(elem, schema.fields)
	newEntity.entity = value.Interface().(*E)
	id := schema.uuid()
	newEntity.id = id
	elem.Field(0).SetUint(id)
	newEntity.value = value
	ci := c.(*contextImplementation)
	ci.trackedEntities = append(ci.trackedEntities, newEntity)
	return newEntity
}

func DeleteEntity[E any](c Context, source *E) RemovableEntity[E] {
	toRemove := &removableEntity[E]{}
	toRemove.c = c
	toRemove.source = source
	toRemove.id = reflect.ValueOf(source).Elem().Field(0).Uint()
	ci := c.(*contextImplementation)
	schema := getEntitySchema[E](c)
	toRemove.schema = schema
	ci.trackedEntities = append(ci.trackedEntities, toRemove)
	//for name, def := schema.strongReferences {
	//
	//}
	return toRemove
}

func EditEntity[E any](c Context, source *E) EditableEntity[E] {
	writable := Copy[E](c, source).(*editableEntity[E])
	writable.id = writable.value.Elem().Field(0).Uint()
	writable.source = source
	ci := c.(*contextImplementation)
	ci.trackedEntities = append(ci.trackedEntities, writable)
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
