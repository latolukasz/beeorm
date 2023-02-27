package simple_metrics

import (
	"strings"
	"sync"

	"github.com/latolukasz/beeorm/v2"
)

const PluginCode = "github.com/latolukasz/beeorm/plugins/simple_metrics"

type Plugin struct {
	options         *Options
	mySQLLogHandler *mySQLLogHandler
}
type Options struct {
	MySQL      bool
	Redis      bool
	LocalCache bool
}

type mySQLQuery struct {
	counter uint64
	time    uint64
}

type MySQLQueryType uint8

const (
	Query MySQLQueryType = iota
	Insert
	Update
	Delete
	Show
	Alter
	Other
)

type PoolName string
type TableName string
type Lazy bool
type MySQLTableLazyGroup map[Lazy]*mySQLQuery
type MySQLTableGroup map[TableName]MySQLTableLazyGroup
type MySQLFlushTypeGroup map[MySQLQueryType]MySQLTableGroup
type MySQLStats map[PoolName]MySQLFlushTypeGroup

type mySQLLogHandler struct {
	m       sync.Mutex
	queries MySQLStats
}

func (ml *mySQLLogHandler) Handle(log map[string]interface{}) {
	pool := PoolName(log["pool"].(string))
	lazy := Lazy(false)
	meta, hasMeta := log["meta"]
	if hasMeta {
		metaData, isMetaData := meta.(beeorm.Bind)
		if isMetaData && metaData["lazy"] == "1" {
			lazy = true
		}
	}
	time := uint64(log["microseconds"].(int64))
	operation := log["operation"].(string)
	query := strings.ToLower(log["query"].(string))
	splitQuery := strings.Split(query, " ")
	table := TableName("unknown")
	queryType := Other
	switch operation {
	case "SELECT":
		switch splitQuery[0] {
		case "select":
			queryType = Query
			for k, part := range splitQuery[2:] {
				if part == "from" {
					table = ml.clearTableName(splitQuery[k+3])
					break
				}
			}
			break
		case "show":
			queryType = Show
			if splitQuery[1] == "tables" && splitQuery[2] == "like" {
				table = ml.clearTableName(splitQuery[3])
			} else if splitQuery[1] == "create" && splitQuery[2] == "table" {
				table = ml.clearTableName(splitQuery[3])
			} else if splitQuery[1] == "indexes" && splitQuery[2] == "from" {
				table = ml.clearTableName(splitQuery[3])
			}
			break
		}
		break
	case "EXEC":
		switch splitQuery[0] {
		case "update":
			queryType = Update
			table = ml.clearTableName(splitQuery[1])
			break
		case "insert":
			queryType = Insert
			table = ml.clearTableName(splitQuery[2])
			break
		case "delete":
			queryType = Delete
			table = ml.clearTableName(splitQuery[2])
			break
		case "alter":
			queryType = Alter
			table = ml.clearTableName(splitQuery[2])
			break
		case "set":
			queryType = Alter
			break
		}
		break
	}
	ml.m.Lock()
	defer ml.m.Unlock()
	l1 := ml.queries[pool]
	if l1 == nil {
		l1 = MySQLFlushTypeGroup{}
		ml.queries[pool] = l1
	}
	l2 := l1[queryType]
	if l2 == nil {
		l2 = MySQLTableGroup{}
		l1[queryType] = l2
	}
	l3 := l2[table]
	if l3 == nil {
		l3 = MySQLTableLazyGroup{}
		l2[table] = l3
	}
	l4 := l3[lazy]
	if l4 == nil {
		l4 = &mySQLQuery{}
		l3[lazy] = l4
	}
	l4.counter++
	l4.time += time
}

func (ml *mySQLLogHandler) clearTableName(table string) TableName {
	s := strings.Split(table, "(")
	if len(s) > 1 {
		table = s[0]
	}
	s = strings.Split(table, ".")
	name := s[0]
	if len(s) > 1 {
		name = s[1]
	}
	return TableName(strings.Trim(name, "`'"))
}

func Init(options *Options) *Plugin {
	if options == nil {
		options = &Options{}
	}
	plugin := &Plugin{options: options}
	if options.MySQL {
		plugin.mySQLLogHandler = &mySQLLogHandler{
			queries: MySQLStats{},
		}
	}
	return plugin
}

func (p *Plugin) GetCode() string {
	return PluginCode
}

func (p *Plugin) GetMySQLStats() MySQLStats {
	if p.mySQLLogHandler == nil {
		return nil
	}
	return p.mySQLLogHandler.queries
}

func (p *Plugin) ClearStats() {
	if p.options.MySQL {
		p.mySQLLogHandler.m.Lock()
		defer p.mySQLLogHandler.m.Unlock()
		p.mySQLLogHandler.queries = MySQLStats{}
	}
}

func (p *Plugin) PluginInterfaceEngineCreated(engine beeorm.Engine) {
	if p.options.MySQL {
		engine.RegisterQueryLogger(p.mySQLLogHandler, true, false, false)
	}
}
