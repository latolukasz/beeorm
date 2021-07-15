package beeorm

type DirtyEntityEvent interface {
	ID() uint64
	TableSchema() TableSchema
	Added() bool
	Updated() bool
	Deleted() bool
}

type dirtyEvent struct {
	I uint64
	A string
	E string
}

func EventDirtyEntity(e Event) DirtyEntityEvent {
	data := dirtyEvent{}
	e.Unserialize(&data)
	schema := e.(*event).consumer.redis.engine.registry.GetTableSchema(data.E)
	return &dirtyEntityEvent{id: data.I, schema: schema, added: data.A == "i", updated: data.A == "u", deleted: data.A == "d"}
}

type dirtyEntityEvent struct {
	id      uint64
	added   bool
	updated bool
	deleted bool
	schema  TableSchema
}

func (d *dirtyEntityEvent) ID() uint64 {
	return d.id
}

func (d *dirtyEntityEvent) TableSchema() TableSchema {
	return d.schema
}

func (d *dirtyEntityEvent) Added() bool {
	return d.added
}

func (d *dirtyEntityEvent) Updated() bool {
	return d.updated
}

func (d *dirtyEntityEvent) Deleted() bool {
	return d.deleted
}
