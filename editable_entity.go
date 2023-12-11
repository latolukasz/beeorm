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
	getValue() reflect.Value
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

type writableEntity struct {
	c      Context
	schema *entitySchema
}

func (w *writableEntity) Schema() *entitySchema {
	return w.schema
}

type insertableEntity struct {
	writableEntity
	entity any
	id     uint64
	value  reflect.Value
}

func (m *insertableEntity) ID() uint64 {
	return m.id
}

func (m *insertableEntity) getEntity() any {
	return m.entity
}

func (m *insertableEntity) flushType() FlushType {
	return Insert
}

func (m *insertableEntity) getValue() reflect.Value {
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
	writableEntity
	id     uint64
	value  reflect.Value
	source *E
}

func (r *removableEntity[E]) flushType() FlushType {
	return Delete
}

func (r *removableEntity[E]) SourceEntity() *E {
	return r.source
}

func (r *removableEntity[E]) getValue() reflect.Value {
	return r.value
}

type editableEntity[E any] struct {
	writableEntity
	entity      *E
	id          uint64
	value       reflect.Value
	sourceValue reflect.Value
	source      *E
}

type editableFields[E any] struct {
	writableEntity
	id      uint64
	value   reflect.Value
	newBind Bind
	oldBind Bind
}

func (f *editableFields[E]) ID() uint64 {
	return f.id
}

func (f *editableFields[E]) flushType() FlushType {
	return Update
}

func (f *editableFields[E]) getBind() (new, old Bind, err error) {
	return f.newBind, f.oldBind, nil
}

func (f *editableFields[E]) getEntity() any {
	return nil
}

func (f *editableFields[E]) getSourceValue() reflect.Value {
	return f.value
}

func (f *editableFields[E]) getValue() reflect.Value {
	return f.value
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

func NewEntity[E any](c Context) *E {
	return newEntity(c, getEntitySchema[E](c)).(*E)
}

func newEntity(c Context, schema *entitySchema) any {
	entity := &insertableEntity{}
	entity.c = c
	entity.schema = schema
	value := reflect.New(schema.t)
	elem := value.Elem()
	initNewEntity(elem, schema.fields)
	entity.entity = value.Interface()
	id := schema.uuid()
	entity.id = id
	elem.Field(0).SetUint(id)
	entity.value = value
	c.trackEntity(entity)
	return entity.getEntity()
}

func DeleteEntity[E any](c Context, source *E) {
	toRemove := &removableEntity[E]{}
	toRemove.c = c
	toRemove.source = source
	toRemove.value = reflect.ValueOf(source).Elem()
	toRemove.id = toRemove.value.Field(0).Uint()
	schema := getEntitySchema[E](c)
	toRemove.schema = schema
	c.trackEntity(toRemove)
}

func EditEntity[E any](c Context, source *E) *E {
	writable := copyToEdit[E](c, source)
	writable.id = writable.value.Elem().Field(0).Uint()
	writable.source = source
	c.trackEntity(writable)
	return writable.entity
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
