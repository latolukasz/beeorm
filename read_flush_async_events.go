package beeorm

import (
	"slices"

	jsoniter "github.com/json-iterator/go"
)

type AsyncFlushEvents interface {
	EntitySchemas() []EntitySchema
	EventsCount() uint64
	ErrorsCount() uint64
	Events(total int) []FlushEvent
	Errors(total int, last bool) []FlushEventWithError
	TrimEvents(total int)
	TrimErrors(total int)
	RedilPool() string
	RedisList() string
}

type asyncFlushEvents struct {
	orm           ORM
	listName      string
	redisPoolName string
	schemas       []EntitySchema
}

type FlushEvent struct {
	SQL             string
	QueryAttributes []string
}

type FlushEventWithError struct {
	FlushEvent
	Error string
}

func (s *asyncFlushEvents) EntitySchemas() []EntitySchema {
	return s.schemas
}

func (s *asyncFlushEvents) RedilPool() string {
	return s.redisPoolName
}

func (s *asyncFlushEvents) RedisList() string {
	return s.listName
}

func (s *asyncFlushEvents) EventsCount() uint64 {
	r := s.orm.Engine().Redis(s.redisPoolName)
	return uint64(r.LLen(s.orm, s.listName))
}

func (s *asyncFlushEvents) ErrorsCount() uint64 {
	r := s.orm.Engine().Redis(s.redisPoolName)
	return uint64(r.LLen(s.orm, s.listName+flushAsyncEventsListErrorSuffix)) / 2
}

func (s *asyncFlushEvents) Events(total int) []FlushEvent {
	r := s.orm.Engine().Redis(s.redisPoolName)
	events := r.LRange(s.orm, s.listName, 0, int64(total-1))
	results := make([]FlushEvent, len(events))
	for i, event := range events {
		var data []string
		_ = jsoniter.ConfigFastest.UnmarshalFromString(event, &data)
		if len(data) > 0 {
			results[i].SQL = data[0]
			if len(data) > 1 {
				results[i].QueryAttributes = data[1:]
			}
		}
	}
	return results
}

func (s *asyncFlushEvents) Errors(total int, last bool) []FlushEventWithError {
	r := s.orm.Engine().Redis(s.redisPoolName)
	var events []string

	if last {
		events = r.LRange(s.orm, s.listName+flushAsyncEventsListErrorSuffix, int64(-total)*2, -1)
	} else {
		events = r.LRange(s.orm, s.listName+flushAsyncEventsListErrorSuffix, 0, int64(total*2-1))
	}
	results := make([]FlushEventWithError, len(events)/2)
	k := 0
	for i, event := range events {
		if i%2 == 0 {
			var data []string
			_ = jsoniter.ConfigFastest.UnmarshalFromString(event, &data)
			if len(data) > 0 {
				results[k].SQL = data[0]
				if len(data) > 1 {
					results[k].QueryAttributes = data[1:]
				}
			}
		} else {
			results[k].Error = event
			k++
		}
	}
	if last {
		slices.Reverse(results)
	}
	return results
}

func (s *asyncFlushEvents) TrimEvents(total int) {
	r := s.orm.Engine().Redis(s.redisPoolName)
	r.Ltrim(s.orm, s.listName, int64(total), int64(-total))
}

func (s *asyncFlushEvents) TrimErrors(total int) {
	total = total * 2
	r := s.orm.Engine().Redis(s.redisPoolName)
	r.Ltrim(s.orm, s.listName+flushAsyncEventsListErrorSuffix, int64(total), int64(-total))
}

func ReadAsyncFlushEvents(orm ORM) []AsyncFlushEvents {
	stats := make([]AsyncFlushEvents, 0)
	mapped := make(map[string]*asyncFlushEvents)
	for _, schema := range orm.Engine().Registry().(*engineRegistryImplementation).entitySchemas {
		stat, has := mapped[schema.asyncCacheKey]
		if has {
			stat.schemas = append(stat.schemas, schema)
			continue
		}
		stat = &asyncFlushEvents{orm: orm, schemas: []EntitySchema{schema}, listName: schema.asyncCacheKey,
			redisPoolName: schema.getForcedRedisCode()}
		stats = append(stats, stat)
		mapped[schema.asyncCacheKey] = stat
	}
	return stats
}
