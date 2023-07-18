package beeorm

import "reflect"

type ID interface {
	int | uint8 | uint16 | uint32 | uint64
}

const cacheNilValue = ""

func initIfNeeded(schema EntitySchema, entity Entity) *ORM {
	orm := entity.getORM()
	if !orm.initialised {
		orm.initialised = true
		value := reflect.ValueOf(entity)
		elem := value.Elem()
		orm.entitySchema = schema
		orm.value = value
		orm.elem = elem
		orm.idElem = elem.Field(1)
	}
	return orm
}
