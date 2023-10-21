package beeorm

type EntityIterator[E any] interface {
	Next() bool
	Len() int
	Entity() *E
	reset()
	all() []*E
}

type localCacheIDsIterator[E any] struct {
	c       *contextImplementation
	ids     []uint64
	index   int
	schema  *entitySchema
	hasRows bool
	rows    []*E
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

func (lc *localCacheIDsIterator[E]) reset() {
	lc.index = -1
}

func (lc *localCacheIDsIterator[E]) all() []*E {
	if lc.hasRows {
		return lc.rows
	}
	lc.rows = make([]*E, lc.Len())
	i := 0
	for lc.Next() {
		lc.rows[i] = lc.Entity()
		i++
	}
	lc.reset()
	lc.hasRows = true
	return lc.rows
}

func (lc *localCacheIDsIterator[E]) Entity() *E {
	if lc.index == -1 {
		return nil
	}
	if lc.hasRows {
		return lc.rows[lc.index]
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

func (el *emptyResultsIterator[E]) reset() {}

func (el *emptyResultsIterator[E]) all() []*E {
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

func (ei *entityIterator[E]) reset() {
	ei.index = -1
}

func (ei *entityIterator[E]) all() []*E {
	return ei.rows
}