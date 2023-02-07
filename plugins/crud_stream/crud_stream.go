package crud_stream

import (
	"time"

	"github.com/latolukasz/beeorm/v2"
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
	schema.SetOption(PluginCode, hasCrudStreamOption, true)
	skip := make([]string, 0)
	for _, columnName := range schema.GetColumns() {
		skipLog := schema.GetTag(columnName, skipCrudStreamOption, "1", "")
		if skipLog == "1" {
			skip = append(skip, columnName)
		}
	}
	if len(skip) > 0 {
		schema.SetOption(PluginCode, skipCrudStreamOption, skip)
	}
	return nil
}

func (p *Plugin) PluginInterfaceInitRegistry(registry *beeorm.Registry) {
	registry.RegisterRedisStream(ChannelName, p.options.DefaultRedisPool)
}

func (p *Plugin) PluginInterfaceEntityFlushed(engine beeorm.Engine, flush *beeorm.EntitySQLFlush, cacheFlusher beeorm.FlusherCacheSetter) {
	entitySchema := engine.GetRegistry().GetEntitySchema(flush.EntityName)
	if entitySchema.GetOption(PluginCode, hasCrudStreamOption) != true {
		return
	}
	skippedFields := entitySchema.GetOption(PluginCode, skipCrudStreamOption)
	if flush.Update != nil && skippedFields != nil {
		skipped := 0
		for _, skip := range skippedFields.([]string) {
			_, has := flush.Update[skip]
			if has {
				skipped++
			}
		}
		if skipped == len(flush.Update) {
			return
		}
	}
	val := &CrudEvent{
		EntityName: flush.EntityName,
		ID:         flush.ID,
		Action:     flush.Action,
		Changes:    flush.Update,
		Updated:    time.Now()}
	if len(flush.Old) > 0 {
		val.Before = flush.Old
	}
	cacheFlusher.PublishToStream(ChannelName, val, flush.Meta)
}

type CrudEvent struct {
	EntityName string
	ID         uint64
	Action     beeorm.FlushType
	Before     beeorm.Bind
	Changes    beeorm.Bind
	Updated    time.Time
}
