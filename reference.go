package beeorm

import (
	"reflect"
)

type IDGetter interface {
	GetID() uint64
}

type referenceInterface interface {
	IDGetter
	getType() reflect.Type
}

type referenceDefinition struct {
	Cached bool
	Type   reflect.Type
}

type Reference[E any] uint64

func (r Reference[E]) GetEntity(orm ORM) *E {
	if r != 0 {
		e, found := GetByID[E](orm, uint64(r))
		if !found {
			return nil
		}
		return e
	}
	return nil
}

func (r Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}

func (r Reference[E]) GetID() uint64 {
	return uint64(r)
}
