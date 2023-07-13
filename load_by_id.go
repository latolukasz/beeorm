package beeorm

import (
	"reflect"
)

const cacheNilValue = ""

func initIfNeeded(schema *entitySchema, entity Entity) *ORM {
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
