package beeorm

const (
	commandDelete = iota
	commandXAdd   = iota
	commandHSet   = iota
)

type redisFlusherCommands struct {
	diffs   map[int]bool
	usePool bool
	deletes []string
	hSets   map[string][]interface{}
	events  map[string][][]string
}

type redisFlusher struct {
	engine    *Engine
	pipelines map[string]*redisFlusherCommands
}

func (f *redisFlusher) Del(redisPool string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if f.pipelines == nil {
		f.pipelines = make(map[string]*redisFlusherCommands)
	}
	commands, has := f.pipelines[redisPool]
	if !has {
		commands = &redisFlusherCommands{deletes: keys, diffs: map[int]bool{commandDelete: true}}
		f.pipelines[redisPool] = commands
		return
	}
	commands.diffs[commandDelete] = true
	commands.deletes = append(commands.deletes, keys...)
}

func (f *redisFlusher) Publish(stream string, body interface{}, meta ...string) {
	eventRaw := createEventSlice(body, meta)
	if f.pipelines == nil {
		f.pipelines = make(map[string]*redisFlusherCommands)
	}
	r := getRedisForStream(f.engine, stream)
	commands, has := f.pipelines[r.config.GetCode()]
	if !has {
		commands = &redisFlusherCommands{events: map[string][][]string{stream: {eventRaw}}, diffs: map[int]bool{commandXAdd: true}}
		f.pipelines[r.config.GetCode()] = commands
		return
	}
	commands.diffs[commandXAdd] = true
	if commands.events == nil {
		commands.events = map[string][][]string{stream: {eventRaw}}
		return
	}
	if commands.events[stream] == nil {
		commands.events[stream] = [][]string{eventRaw}
		return
	}
	commands.events[stream] = append(commands.events[stream], eventRaw)
	commands.usePool = true
}

func (f *redisFlusher) HSet(redisPool, key string, values ...interface{}) {
	if f.pipelines == nil {
		f.pipelines = make(map[string]*redisFlusherCommands)
	}
	commands, has := f.pipelines[redisPool]
	if !has {
		val := map[string][]interface{}{key: values}
		commands = &redisFlusherCommands{hSets: val, diffs: map[int]bool{commandHSet: true}}
		f.pipelines[redisPool] = commands
		return
	}
	commands.diffs[commandHSet] = true
	if commands.hSets == nil {
		commands.hSets = map[string][]interface{}{key: values}
		return
	}
	commands.hSets[key] = append(commands.hSets[key], values...)
}

func (f *redisFlusher) Flush() {
	if len(f.pipelines) <= 1 {
		for poolCode, commands := range f.pipelines {
			usePool := commands.usePool || len(commands.diffs) > 1 || len(commands.events) > 1 || len(commands.hSets) > 1
			if usePool {
				p := f.engine.GetRedis(poolCode).PipeLine()
				if commands.deletes != nil {
					p.Del(commands.deletes...)
				}
				for key, values := range commands.hSets {
					p.HSet(key, values...)
				}
				for stream, events := range commands.events {
					for _, e := range events {
						p.XAdd(stream, e)
					}
				}
				p.Exec()
			} else {
				r := f.engine.GetRedis(poolCode)
				if commands.deletes != nil {
					r.Del(commands.deletes...)
				}
				if commands.hSets != nil {
					for key, values := range commands.hSets {
						r.HSet(key, values...)
					}
				}
				for stream, events := range commands.events {
					for _, e := range events {
						r.xAdd(stream, e)
					}
				}
			}
		}
		f.pipelines = nil
		return
	}
	for poolCode, commands := range f.pipelines {
		usePool := commands.usePool || len(commands.diffs) > 1 || len(commands.hSets) > 1
		if usePool {
			p := f.engine.GetRedis(poolCode).PipeLine()
			has := false
			if commands.deletes != nil {
				p.Del(commands.deletes...)
				has = true
			}
			for key, values := range commands.hSets {
				p.HSet(key, values...)
				has = true
			}
			if has {
				p.Exec()
			}
		} else {
			r := f.engine.GetRedis(poolCode)
			if commands.deletes != nil {
				r.Del(commands.deletes...)
			}
			if commands.hSets != nil {
				for key, values := range commands.hSets {
					r.HSet(key, values...)
				}
			}
		}
	}
	for poolCode, commands := range f.pipelines {
		if len(commands.events) == 0 {
			continue
		}
		usePool := len(commands.events) > 1
		if usePool {
			p := f.engine.GetRedis(poolCode).PipeLine()
			for stream, events := range commands.events {
				for _, e := range events {
					p.XAdd(stream, e)
				}
			}
			p.Exec()
		} else {
			r := f.engine.GetRedis(poolCode)
			for stream, events := range commands.events {
				for _, e := range events {
					r.xAdd(stream, e)
				}
			}
		}
	}
	f.pipelines = nil
}
