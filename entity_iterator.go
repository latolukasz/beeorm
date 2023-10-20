package beeorm

type EntityIterator[E any] interface {
	Next() bool
	Len() int
	Entity() *E
}

type localCacheIDsIterator[E any] struct {
	c      *contextImplementation
	ids    []uint64
	index  int
	schema *entitySchema
}

func (lc *localCacheIDsIterator[E]) Next() bool {
	if lc.index+1 >= len(lc.ids) {
		return false
	}
	lc.index++
	return true
}

func (lc *localCacheIDsIterator[E]) Len() int {
	return len(lc.ids)
}

func (lc *localCacheIDsIterator[E]) Entity() *E {
	if lc.index == -1 {
		return nil
	}
	return getByID[E](lc.c, lc.ids[lc.index], lc.schema)
}

type emptyResultsIterator[E any] struct{}

func (el *emptyResultsIterator[E]) Next() bool {
	return false
}

func (el *emptyResultsIterator[E]) Len() int {
	return 0
}

func (el *emptyResultsIterator[E]) Entity() *E {
	return nil
}

type entityIterator[E any] struct {
	index int
	rows  []*E
}

func (ei *entityIterator[E]) Next() bool {
	if ei.index+1 >= len(ei.rows) {
		return false
	}
	ei.index++
	return true
}

func (ei *entityIterator[E]) Len() int {
	return len(ei.rows)
}

func (ei *entityIterator[E]) Entity() *E {
	if ei.index == -1 {
		return nil
	}
	return ei.rows[ei.index]
}
