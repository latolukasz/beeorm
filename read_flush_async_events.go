package beeorm

import jsoniter "github.com/json-iterator/go"

type AsyncFlushEvents interface {
	EntitySchemas() []EntitySchema
	EventsCount() uint64
	ErrorsCount() uint64
	Events(total int) []FlushEvent
	Errors(total int) []FlushEventWithError
	TrimEvents(total int)
	TrimErrors(total int)
}

type asyncFlushEvents struct {
	c             Context
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

func (s *asyncFlushEvents) EventsCount() uint64 {
	r := s.c.Engine().Redis(s.redisPoolName)
	return uint64(r.LLen(s.c, s.listName))
}

func (s *asyncFlushEvents) ErrorsCount() uint64 {
	r := s.c.Engine().Redis(s.redisPoolName)
	return uint64(r.LLen(s.c, s.listName+flushAsyncEventsListErrorSuffix)) / 2
}

func (s *asyncFlushEvents) Events(total int) []FlushEvent {
	r := s.c.Engine().Redis(s.redisPoolName)
	events := r.LRange(s.c, s.listName, 0, int64(total-1))
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

func (s *asyncFlushEvents) Errors(total int) []FlushEventWithError {
	r := s.c.Engine().Redis(s.redisPoolName)
	events := r.LRange(s.c, s.listName+flushAsyncEventsListErrorSuffix, 0, int64(total*2-1))
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
	return results
}

func (s *asyncFlushEvents) TrimEvents(total int) {
	r := s.c.Engine().Redis(s.redisPoolName)
	r.Ltrim(s.c, s.listName, int64(total), int64(-total))
}

func (s *asyncFlushEvents) TrimErrors(total int) {
	total = total * 2
	r := s.c.Engine().Redis(s.redisPoolName)
	r.Ltrim(s.c, s.listName+flushAsyncEventsListErrorSuffix, int64(total), int64(-total))
}

func ReadAsyncFlushEvents(c Context) []AsyncFlushEvents {
	stats := make([]AsyncFlushEvents, 0)
	mapped := make(map[string]*asyncFlushEvents)
	for _, schema := range c.Engine().Registry().(*engineRegistryImplementation).entitySchemas {
		stat, has := mapped[schema.asyncCacheKey]
		if has {
			stat.schemas = append(stat.schemas, schema)
			continue
		}
		stat = &asyncFlushEvents{c: c, schemas: []EntitySchema{schema}, listName: schema.asyncCacheKey,
			redisPoolName: schema.getForcedRedisCode()}
		stats = append(stats, stat)
		mapped[schema.asyncCacheKey] = stat
	}
	return stats
}
