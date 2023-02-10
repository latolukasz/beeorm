package uuid

import (
	"github.com/latolukasz/beeorm/v2"
	"sync/atomic"
	"time"
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
	hasUUID := schema.GetTag("ORM", p.options.TagName, "true", "")
	if hasUUID != "true" {
		return nil
	}
	schema.SetPluginOption(PluginCode, hasUUIDOption, "true")
	return nil
}
