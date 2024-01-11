package beeorm

import (
	"reflect"
	"time"
)

type LogEntity[Entity any] struct {
	ID       uint64 `orm:"split_async_flush=log-entity"`
	EntityID uint64
	Date     time.Time `orm:"time"`
	Meta     []byte
	Before   []byte
	After    []byte
}

type logEntityInterface interface {
	getLogEntityTarget() reflect.Type
}

func (l *LogEntity[Entity]) getLogEntityTarget() reflect.Type {
	var e Entity
	return reflect.TypeOf(e)
}
