package uuid

import (
	"sync/atomic"
	"time"

	"github.com/latolukasz/beeorm/v2"
)

const PluginCode = "github.com/latolukasz/beeorm/plugins/uuid"
const defaultTagName = "uuid"
const hasUUIDOption = "has-uuid"

var codeStartTime = uint64(time.Now().Unix())

func (p *Plugin) uuid() uint64 {
	return (p.options.UUIDServerID&255)<<56 + (codeStartTime << 24) + atomic.AddUint64(&p.options.uuidCounter, 1)
}

type Plugin struct {
	options *Options
}
type Options struct {
	TagName      string
	UUIDServerID uint64
	uuidCounter  uint64
}

func Init(options *Options) *Plugin {
	if options == nil {
		options = &Options{}
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
	if !p.hasUUID(schema) {
		return nil
	}
	schema.SetPluginOption(PluginCode, hasUUIDOption, "true")
	return nil
}

func (p *Plugin) PluginInterfaceTableSQLSchemaDefinition(engine beeorm.Engine, sqlSchema *beeorm.TableSQLSchemaDefinition) error {
	mySQLVersion := sqlSchema.EntitySchema.GetMysql(engine).GetPoolConfig().GetVersion()
	if mySQLVersion == 8 {
		sqlSchema.EntityColumns[0].Definition = "`ID` bigint unsigned NOT NULL"
	} else {
		sqlSchema.EntityColumns[0].Definition = "`ID` bigint(20) unsigned NOT NULL"
	}
	return nil
}

func (p *Plugin) PluginInterfaceEntityFlushing(engine beeorm.Engine, event beeorm.EventEntityFlushing) {
	if !event.Type().Is(beeorm.Insert) || event.EntityID() > 0 {
		return
	}
	schema := engine.GetRegistry().GetEntitySchema(event.EntityName())
	if !p.hasUUID(schema) {
		return
	}
	event.SetID(p.uuid())
}

func (p *Plugin) hasUUID(schema beeorm.EntitySchema) bool {
	return schema.GetTag("ORM", p.options.TagName, "true", "") == "true"
}
