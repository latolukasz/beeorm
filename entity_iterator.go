package beeorm

import (
	"fmt"
	"reflect"
	"strings"
)

type EntityIterator[E any] interface {
	Next() bool
	Len() int
	Entity() *E
	All() []*E
	Reset()
	LoadReference(columns ...string)
}

type EntityAnonymousIterator interface {
	Next() bool
	Len() int
	Entity() any
	Reset()
}

type localCacheIDsIterator[E any] struct {
	orm     *ormImplementation
	ids     []uint64
	index   int
	schema  *entitySchema
	hasRows bool
	rows    []*E
}

func (lc *localCacheIDsIterator[E]) Next() bool {
	if lc.index+1 >= len(lc.ids) {
		lc.Reset()
		return false
	}
	lc.index++
	return true
}

func (lc *localCacheIDsIterator[E]) Len() int {
	return len(lc.ids)
}

func (lc *localCacheIDsIterator[E]) Reset() {
	lc.index = -1
}

func (lc *localCacheIDsIterator[E]) All() []*E {
	if lc.hasRows {
		return lc.rows
	}
	lc.rows = make([]*E, lc.Len())
	i := 0
	for lc.Next() {
		lc.rows[i] = lc.Entity()
		i++
	}
	lc.Reset()
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
	if lc.index == 0 {
		value, hit := lc.schema.localCache.getEntity(lc.orm, lc.ids[0])
		if hit {
			if value == nil {
				return nil
			}
			return value.(*E)
		}
		lc.warmup()
	}
	value, found := getByID(lc.orm, lc.ids[lc.index], lc.schema)
	if !found {
		return nil
	}
	return value.(*E)
}

func (lc *localCacheIDsIterator[E]) LoadReference(columns ...string) {
	if lc.Len() <= 1 {
		return
	}
	var ids []uint64
	for _, row := range columns {
		fields := strings.Split(row, "/")
		reference, has := lc.schema.references[fields[0]]
		if !has {
			panic(fmt.Errorf("invalid reference name %s", row))
		}
		index := lc.index
		lc.index = -1
		for lc.Next() {
			entity := reflect.ValueOf(lc.Entity()).Elem()
			field := entity.FieldByName(fields[0])
			if field.IsNil() {
				continue
			}
			field = field.Elem()
			id := field.FieldByName("ID").Uint()
			has = false
			for _, before := range ids {
				if before == id {
					has = true
					break
				}
			}
			if !has {
				ids = append(ids, id)
			}
		}
		lc.index = index
		if len(ids) <= 1 {
			return
		}
		refSchema := lc.orm.Engine().Registry().EntitySchema(reference.Type).(*entitySchema)
		warmup(lc.orm, refSchema, ids)
	}
}

func (lc *localCacheIDsIterator[E]) warmup() {
	if len(lc.ids)-lc.index <= 2 {
		return
	}
	warmup(lc.orm, lc.schema, lc.ids)
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

func (el *emptyResultsIterator[E]) Reset() {}

func (el *emptyResultsIterator[E]) All() []*E {
	return nil
}

func (el *emptyResultsIterator[E]) LoadReference(_ ...string) {

}

type entityIterator[E any] struct {
	index int
	rows  []*E
}

func (ei *entityIterator[E]) Next() bool {
	if ei.index+1 >= len(ei.rows) {
		ei.Reset()
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

func (ei *entityIterator[E]) Reset() {
	ei.index = -1
}

func (ei *entityIterator[E]) All() []*E {
	return ei.rows
}

func (ei *entityIterator[E]) LoadReference(_ ...string) {

}

type entityAnonymousIterator struct {
	index int
	rows  reflect.Value
}

func (ea *entityAnonymousIterator) Next() bool {
	if ea.index+1 >= ea.rows.Len() {
		ea.Reset()
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

func (ea *entityAnonymousIterator) Reset() {
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

func (el *emptyResultsAnonymousIterator) Reset() {}

var emptyResultsAnonymousIteratorInstance = &emptyResultsAnonymousIterator{}

type localCacheIDsAnonymousIterator struct {
	c      *ormImplementation
	ids    []uint64
	index  int
	schema *entitySchema
}

func (lc *localCacheIDsAnonymousIterator) Next() bool {
	if lc.index+1 >= len(lc.ids) {
		lc.Reset()
		return false
	}
	lc.index++
	return true
}

func (lc *localCacheIDsAnonymousIterator) Len() int {
	return len(lc.ids)
}

func (lc *localCacheIDsAnonymousIterator) Reset() {
	lc.index = -1
}

func (lc *localCacheIDsAnonymousIterator) Entity() any {
	if lc.index == -1 {
		return nil
	}
	value, found := getByID(lc.c, lc.ids[lc.index], lc.schema)
	if !found {
		return nil
	}
	return value
}
