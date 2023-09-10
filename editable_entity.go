package beeorm

import (
	"reflect"
	"slices"
)

type FlushType int

const (
	Insert FlushType = iota
	Update
	Delete
)

type Meta map[string]string

type EntityFlushEvent interface {
	FlushType() FlushType
	GetMetaData() Meta
}

type EntityFlushedEvent interface {
	FlushType() FlushType
	GetMetaData() Meta
}

func (m Meta) Get(key string) string {
	return m[key]
}

type writableEntityInterface[E Entity] interface {
	Entity() E
	SetMetaData(key, value string)
	GetMetaData() Meta
}

type InsertableEntity[E Entity] interface {
	writableEntityInterface[E]
	SetOnDuplicateKeyUpdate(bind Bind)
	GetBind() Bind
}

type EditableEntity[E Entity] interface {
	writableEntityInterface[E]
	Source() E
	Delete()
	GetBind() (new, old Bind)
}

type writableEntity[E Entity] struct {
	c      Context
	entity E
	value  reflect.Value
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

func (w *writableEntity[E]) GetMetaData() Meta {
	return w.meta
}

type insertableEntity[E Entity] struct {
	writableEntity[E]
	onDuplicateKeyUpdate Bind
}

func (m *insertableEntity[E]) SetOnDuplicateKeyUpdate(bind Bind) {
	m.onDuplicateKeyUpdate = bind
}

func (m *insertableEntity[E]) FlushType() FlushType {
	return Insert
}

func (e *editableEntity[E]) FlushType() FlushType {
	if e.delete {
		return Delete
	}
	return Update
}

type editableEntity[E Entity] struct {
	writableEntity[E]
	source E
	delete bool
}

func (w *writableEntity[E]) Entity() E {
	return w.entity
}

func (e *editableEntity[E]) Delete() {
	e.delete = true
}

func (e *editableEntity[E]) Source() E {
	return e.source
}

func NewEntity[E Entity](c Context) InsertableEntity[E] {
	newEntity := &insertableEntity[E]{}
	newEntity.entity = initNewEntity[E](c)
	ci := c.(*contextImplementation)
	ci.trackedEntities = append(ci.trackedEntities, newEntity)
	return newEntity
}

func CloneForEdit[E Entity](c Context, source E) EditableEntity[E] {
	cloned := Copy[E](c, source)
	writable := &editableEntity[E]{}
	writable.entity = cloned
	writable.source = source
	ci := c.(*contextImplementation)
	ci.trackedEntities = append(ci.trackedEntities, writable)
	return writable
}

func Copy[E Entity](c Context, source E) E {
	cloned := initNewEntity[E](c)
	cloned.getORM().binary = slices.Clone(source.getORM().binary)
	cloned.getORM().deserialize(c)
	return cloned
}

func initNewEntity[E Entity](c Context) E {
	schema := GetEntitySchema[E](c).(*entitySchema)
	val := reflect.New(schema.t)
	e := val.Interface().(Entity)
	orm := e.getORM()
	orm.value = val
	orm.elem = val.Elem()
	orm.idElem = orm.elem.Field(1)
	return e.(E)
}
