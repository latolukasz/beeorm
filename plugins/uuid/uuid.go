package uuid

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/latolukasz/beeorm/v3"
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
	if schema.GetTag("ORM", p.options.TagName, "true", "") != "true" {
		return nil
	}
	if schema.GetType().Field(1).Type.String() != "uint64" {
		return fmt.Errorf("ID field in %s must be uint64", schema.GetType().String())
	}
	schema.SetPluginOption(PluginCode, hasUUIDOption, true)
	return nil
}

func (p *Plugin) PluginInterfaceTableSQLSchemaDefinition(_ beeorm.Context, sqlSchema *beeorm.TableSQLSchemaDefinition) error {
	if sqlSchema.EntitySchema.GetPluginOption(PluginCode, hasUUIDOption) != true {
		return nil
	}
	mySQLVersion := sqlSchema.EntitySchema.GetDB().GetPoolConfig().GetVersion()
	if mySQLVersion == 8 {
		sqlSchema.EntityColumns[0].Definition = "`ID` bigint unsigned NOT NULL"
	} else {
		sqlSchema.EntityColumns[0].Definition = "`ID` bigint(20) unsigned NOT NULL"
	}
	return nil
}

func (p *Plugin) PluginInterfaceEntityFlushing(c beeorm.Context, event beeorm.EventEntityFlushing) {
	if !event.Type().Is(beeorm.Insert) || event.EntityID() > 0 {
		return
	}
	schema := c.Engine().Registry().EntitySchema(event.EntityName())
	if schema.GetPluginOption(PluginCode, hasUUIDOption) != true {
		return
	}
	event.SetID(p.uuid())
}
