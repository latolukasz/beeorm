package beeorm

import (
	"reflect"
)

type referenceInterface interface {
	getID() uint64
	setID(id uint64)
	getType() reflect.Type
}

type referenceDefinition struct {
	Cached bool
	Type   reflect.Type
}

type Reference[E any] struct {
	ID uint64
}

func (r *Reference[E]) GetEntity(orm ORM) *E {
	if r.ID != 0 {
		return GetByID[E](orm, r.ID)
	}
	return nil
}

func (r *Reference[E]) getID() uint64 {
	return r.ID
}

func (r *Reference[E]) setID(id uint64) {
	r.ID = id
}

func (r *Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}
