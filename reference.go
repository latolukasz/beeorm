package beeorm

type Reference[E Entity] struct {
	schema EntitySchema
	id     uint64
	e      E
}

func (r *Reference[E]) Entity(c Context) E {
	return r.e
}

func (r *Reference[E]) ID() uint64 {
	return r.id
}
