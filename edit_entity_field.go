package beeorm

import (
	"fmt"
	"reflect"
)

func EditEntityField[E any](c Context, entity *E, field string, value any, execute bool) error {
	schema := getEntitySchema[E](c)
	setter, has := schema.fieldBindSetters[field]
	if !has {
		return fmt.Errorf("field '%s' not found", field)
	}
	bindValue, err := setter(value)
	if err != nil {
		return err
	}
	if execute {
		elem := reflect.ValueOf(entity).Elem()
		sql := "UPDATE `" + schema.GetTableName() + "` SET `" + field + "` = ? WHERE ID = ?"
		schema.GetDB().Exec(c, sql, bindValue, elem.Field(0).Uint())
		fSetter := schema.fieldSetters[field]
		if schema.hasLocalCache {
			func() {
				schema.localCache.mutex.Lock()
				defer schema.localCache.mutex.Unlock()
				fSetter(bindValue, elem)
			}()
		} else {
			fSetter(bindValue, elem)
		}
	}
	return nil
}
