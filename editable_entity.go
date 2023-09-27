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
}

type EntityFlushedEvent interface {
	FlushType() FlushType
}
type writableEntityInterface[E Entity] interface {
}

type InsertableEntity[E Entity] interface {
	writableEntityInterface[E]
	TrackedEntity() E
	getBind() (Bind, error)
}

type RemovableEntity[E Entity] interface {
	writableEntityInterface[E]
	SourceEntity() E
}

type EditableEntity[E Entity] interface {
	writableEntityInterface[E]
	TrackedEntity() E
	SourceEntity() E
	getBind() (new, old Bind, err error)
}

type writableEntity[E Entity] struct {
	c      Context
	schema *entitySchema
}

func (w *writableEntity[E]) Schema() *entitySchema {
	return w.schema
}

type insertableEntity[E Entity] struct {
	writableEntity[E]
	entity E
	value  reflect.Value
}

func (m *insertableEntity[E]) ID() uint64 {
	return m.entity.GetID()
}

func (m *insertableEntity[E]) TrackedEntity() E {
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

func (e *editableEntity[E]) getSourceValue() reflect.Value {
	return e.sourceValue
}

type removableEntity[E Entity] struct {
	writableEntity[E]
	source E
}

func (r *removableEntity[E]) flushType() FlushType {
	return Delete
}

func (r *removableEntity[E]) SourceEntity() E {
	return r.source
}

type editableEntity[E Entity] struct {
	writableEntity[E]
	entity      E
	value       reflect.Value
	sourceValue reflect.Value
	source      E
}

func (e *editableEntity[E]) ID() uint64 {
	return e.entity.GetID()
}

func (r *removableEntity[E]) ID() uint64 {
	return r.source.GetID()
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

func DeleteEntity[E Entity](c Context, source E) RemovableEntity[E] {
	toRemove := &removableEntity[E]{}
	toRemove.c = c
	toRemove.source = source
	ci := c.(*contextImplementation)
	schema := GetEntitySchema[E](c).(*entitySchema)
	toRemove.schema = schema
	ci.trackedEntities = append(ci.trackedEntities, toRemove)
	return toRemove
}

func EditEdit[E Entity](c Context, source E) EditableEntity[E] {
	writable := Copy[E](c, source).(*editableEntity[E])
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
