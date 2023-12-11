package beeorm

import "reflect"

type EntityIterator[E any] interface {
	Next() bool
	Len() int
	Entity() *E
	reset()
	all() []*E
}

type EntityAnonymousIterator interface {
	Next() bool
	Len() int
	Entity() any
	reset()
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
	value := getByID(lc.c, lc.ids[lc.index], lc.schema)
	if value == nil {
		return nil
	}
	return value.(*E)
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

type entityAnonymousIterator struct {
	index int
	rows  reflect.Value
}

func (ea *entityAnonymousIterator) Next() bool {
	if ea.index+1 >= ea.rows.Len() {
		return false
	}
	ea.index++
	return true
}

func (ea *entityAnonymousIterator) Len() int {
	return ea.rows.Len()
}

func (ea *entityAnonymousIterator) Entity() any {
	if ea.index == -1 {
		return nil
	}
	return ea.rows.Index(ea.index).Interface()
}

func (ea *entityAnonymousIterator) reset() {
	ea.index = -1
}

type emptyResultsAnonymousIterator struct{}

func (el *emptyResultsAnonymousIterator) Next() bool {
	return false
}

func (el *emptyResultsAnonymousIterator) Len() int {
	return 0
}

func (el *emptyResultsAnonymousIterator) Entity() any {
	return nil
}

func (el *emptyResultsAnonymousIterator) reset() {}

var emptyResultsAnonymousIteratorInstance = &emptyResultsAnonymousIterator{}

type localCacheIDsAnonymousIterator struct {
	c      *contextImplementation
	ids    []uint64
	index  int
	schema *entitySchema
}

func (lc *localCacheIDsAnonymousIterator) Next() bool {
	if lc.index+1 >= len(lc.ids) {
		return false
	}
	lc.index++
	return true
}

func (lc *localCacheIDsAnonymousIterator) Len() int {
	return len(lc.ids)
}

func (lc *localCacheIDsAnonymousIterator) reset() {
	lc.index = -1
}

func (lc *localCacheIDsAnonymousIterator) Entity() any {
	if lc.index == -1 {
		return nil
	}
	value := getByID(lc.c, lc.ids[lc.index], lc.schema)
	if value == nil {
		return nil
	}
	return value
}
