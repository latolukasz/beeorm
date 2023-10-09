package beeorm

import "reflect"

type referenceInterface interface {
	GetID() uint64
	SetID(id uint64)
	getType() reflect.Type
}

type referenceDefinition struct {
	Cached bool
	Type   reflect.Type
}

func NewReference[E any](id uint64) *Reference[E] {
	return &Reference[E]{id: id}
}

type Reference[E any] struct {
	id uint64
}

func (r *Reference[E]) GetEntity(c Context) *E {
	if r.id != 0 {
		return GetByID[E](c, r.id)
	}
	return nil
}

func (r *Reference[E]) GetID() uint64 {
	return r.id
}

func (r *Reference[E]) SetID(id uint64) {
	r.id = id
}

func (r *Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}
