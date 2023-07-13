package crud_stream

import (
	"time"

	"github.com/latolukasz/beeorm/v3"
)

const PluginCode = "github.com/latolukasz/beeorm/plugins/crud_stream"
const ChannelName = "beeorm-crud-stream"
const defaultTagName = "crud-stream"
const hasCrudStreamOption = "has-crud-stream"
const skipCrudStreamOption = "skip-crud-stream"

type Plugin struct {
	options *Options
}

type Options struct {
	TagName          string
	DefaultRedisPool string
}

func Init(options *Options) *Plugin {
	if options == nil {
		options = &Options{}
	}
	if options.DefaultRedisPool == "" {
		options.DefaultRedisPool = "default"
	}
	if options.TagName == "" {
		options.TagName = defaultTagName
	}
	return &Plugin{options}
}

func (p *Plugin) GetCode() string {
	return PluginCode
}

func (p *Plugin) InterfaceInitEntitySchema(schema beeorm.SettableEntitySchema, _ *beeorm.Registry) error {
	crudStream := schema.GetTag("ORM", p.options.TagName, "true", "false")
	if crudStream != "true" {
		return nil
	}
	schema.SetPluginOption(PluginCode, hasCrudStreamOption, true)
	skip := make([]string, 0)
	for _, columnName := range schema.GetColumns() {
		skipLog := schema.GetTag(columnName, skipCrudStreamOption, "1", "")
		if skipLog == "1" {
			skip = append(skip, columnName)
		}
	}
	if len(skip) > 0 {
		schema.SetPluginOption(PluginCode, skipCrudStreamOption, skip)
	}
	return nil
}

func (p *Plugin) PluginInterfaceInitRegistry(registry *beeorm.Registry) {
	registry.RegisterRedisStream(ChannelName, p.options.DefaultRedisPool)
}

func (p *Plugin) PluginInterfaceEntityFlushing(engine beeorm.Engine, event beeorm.EventEntityFlushing) {
	metaData := engine.GetMetaData()
	if metaData != nil {
		for key, value := range metaData {
			event.SetMetaData(key, value)
		}
	}
}

func (p *Plugin) PluginInterfaceEntityFlushed(engine beeorm.Engine, event beeorm.EventEntityFlushed, cacheFlusher beeorm.FlusherCacheSetter) {
	entitySchema := engine.GetRegistry().GetEntitySchema(event.EntityName())
	if entitySchema.GetPluginOption(PluginCode, hasCrudStreamOption) != true {
		return
	}
	skippedFields := entitySchema.GetPluginOption(PluginCode, skipCrudStreamOption)
	if event.After() != nil && skippedFields != nil {
		skipped := 0
		for _, skip := range skippedFields.([]string) {
			_, has := event.After()[skip]
			if has {
				skipped++
			}
		}
		if skipped == len(event.After()) {
			return
		}
	}
	val := &CrudEvent{
		EntityName: event.EntityName(),
		ID:         event.EntityID(),
		Action:     event.Type(),
		Changes:    event.After(),
		MetaData:   event.MetaData(),
		Updated:    time.Now()}
	if len(event.Before()) > 0 {
		val.Before = event.Before()
	}
	cacheFlusher.PublishToStream(ChannelName, val, nil)
}

type CrudEvent struct {
	EntityName string
	ID         uint64
	Action     beeorm.FlushType
	Before     beeorm.Bind
	Changes    beeorm.Bind
	MetaData   beeorm.Meta
	Updated    time.Time
}
