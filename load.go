package beeorm

func Load(c Context, entity Entity, references ...string) bool {
	if entity.IsLoaded() {
		//if len(references) > 0 {
		//	orm := entity.getORM()
		//	warmUpReferences(serializer, e, orm.entitySchema, orm.elem, references, false)
		//}
		return true
	}
	schema := GetEntitySchema[Entity](c)
	orm := initIfNeeded(schema.(*entitySchema), entity)
	id := orm.GetID()
	if id > 0 {
		return getByID[Entity](c, id, entity, references...) != nil
	}
	return false
}