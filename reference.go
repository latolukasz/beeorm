package beeorm

type referenceInterface interface {
	GetID() uint64
	SetID(id uint64)
}

func NewReference[E Entity](id uint64) *Reference[E] {
	return &Reference[E]{id: id}
}

type Reference[E Entity] struct {
	schema EntitySchema
	id     uint64
}

func (r *Reference[E]) GetEntity(c Context) E {
	if r.id != 0 {
		return GetByID[E](c, r.id)
	}
	return *new(E)
}

func (r *Reference[E]) EntitySchema() EntitySchema {
	return nil
}

func (r *Reference[E]) GetID() uint64 {
	return r.id
}

func (r *Reference[E]) SetID(id uint64) {
	r.id = id
}
