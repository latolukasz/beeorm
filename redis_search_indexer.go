package beeorm

import (
	"strconv"
	"strings"
)

type redisIndexerEvent struct {
	Index   string
	IndexID uint64
}

type RedisSearchIndexPusher interface {
	NewDocument(key string)
	DeleteDocuments(key ...string)
	SetString(key string, value string)
	SetTag(key string, tag ...string)
	SetUint(key string, value uint64)
	SetInt(key string, value int64)
	SetFloat(key string, value float64)
	SetGeo(key string, lon float64, lat float64)
	PushDocument()
	Flush()
	setField(key string, value interface{})
}

type RedisSearchIndexerFunc func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool)

type redisSearchIndexPusher struct {
	pipeline *RedisPipeLine
	key      string
	fields   []interface{}
}

func (e *Engine) NewRedisSearchIndexPusher(pool string) RedisSearchIndexPusher {
	return &redisSearchIndexPusher{pipeline: e.GetRedis(pool).PipeLine()}
}

func (p *redisSearchIndexPusher) NewDocument(key string) {
	p.key = key
}

func (p *redisSearchIndexPusher) DeleteDocuments(key ...string) {
	p.pipeline.Del(key...)
}

func (p *redisSearchIndexPusher) SetString(key string, value string) {
	p.fields = append(p.fields, key, EscapeRedisSearchString(value))
}

func (p *redisSearchIndexPusher) setField(key string, value interface{}) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetTag(key string, tag ...string) {
	for i, val := range tag {
		if val == "" {
			tag[i] = "NULL"
		} else {
			tag[i] = EscapeRedisSearchString(val)
		}
	}
	p.fields = append(p.fields, key, strings.Join(tag, ","))
}

func (p *redisSearchIndexPusher) SetUint(key string, value uint64) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetInt(key string, value int64) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetFloat(key string, value float64) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetGeo(key string, lon float64, lat float64) {
	lonS := strconv.FormatFloat(lon, 'f', 6, 64)
	latS := strconv.FormatFloat(lat, 'f', 6, 64)
	p.fields = append(p.fields, key, lonS+","+latS)
}

func (p *redisSearchIndexPusher) PushDocument() {
	p.pipeline.HSet(p.key, p.fields...)
	p.key = ""
	p.fields = p.fields[:0]
	if p.pipeline.commands > 10000 {
		p.Flush()
	}
}

func (p *redisSearchIndexPusher) Flush() {
	if p.pipeline.commands > 0 {
		p.pipeline.Exec()
	}
}
