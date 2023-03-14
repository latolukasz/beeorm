package log_table

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/latolukasz/beeorm/v2/plugins/crud_stream"

	"github.com/go-sql-driver/mysql"
	"github.com/latolukasz/beeorm/v2"

	jsoniter "github.com/json-iterator/go"
)

const PluginCode = "github.com/latolukasz/beeorm/plugins/log_table"
const ConsumerGroupName = "log-tables-consumer"
const defaultTagName = "log-table"
const poolOption = "pool"
const tableNameOption = "log-table"

type Plugin struct {
	options *Options
}
type Options struct {
	TagName          string
	DefaultMySQLPool string
}

func Init(options *Options) *Plugin {
	if options == nil {
		options = &Options{}
	}
	if options.DefaultMySQLPool == "" {
		options.DefaultMySQLPool = "default"
	}
	if options.TagName == "" {
		options.TagName = defaultTagName
	}
	return &Plugin{options}
}

func (p *Plugin) GetCode() string {
	return PluginCode
}

func (p *Plugin) PluginInterfaceInitRegistry(registry *beeorm.Registry) {
	registry.RegisterRedisStreamConsumerGroups(crud_stream.ChannelName, ConsumerGroupName)
}

func (p *Plugin) InterfaceInitEntitySchema(schema beeorm.SettableEntitySchema, registry *beeorm.Registry) error {
	logPoolName := schema.GetTag("ORM", p.options.TagName, p.options.DefaultMySQLPool, "")
	if logPoolName == "" {
		return nil
	}
	tableName := fmt.Sprintf("_log_%s_%s", schema.GetMysqlPool(), schema.GetTableName())
	schema.SetPluginOption(PluginCode, poolOption, logPoolName)
	schema.SetPluginOption(PluginCode, tableNameOption, tableName)
	registry.RegisterMySQLTable(logPoolName, tableName)
	return nil
}

func (p *Plugin) PluginInterfaceTableSQLSchemaDefinition(engine beeorm.Engine, sqlSchema *beeorm.TableSQLSchemaDefinition) error {
	poolName := sqlSchema.EntitySchema.GetPluginOption(PluginCode, poolOption)
	if poolName == nil {
		return nil
	}
	tableName := sqlSchema.EntitySchema.GetPluginOption(PluginCode, tableNameOption)
	db := engine.GetMysql(poolName.(string))
	var tableDef string
	hasLogTable := db.QueryRow(beeorm.NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)), &tableDef)
	var logEntitySchema string
	if db.GetPoolConfig().GetVersion() == 5 {
		logEntitySchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  "+
			"`entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
			"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
			db.GetPoolConfig().GetDatabase(), tableName)
	} else {
		logEntitySchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  "+
			"`entity_id` int unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
			"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_%s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
			db.GetPoolConfig().GetDatabase(), tableName, engine.GetRegistry().GetSourceRegistry().GetDefaultCollate())
	}

	if !hasLogTable {
		sqlSchema.PostAlters = append(sqlSchema.PostAlters, beeorm.Alter{SQL: logEntitySchema, Safe: true, Pool: poolName.(string)})
	} else {
		var skip, createTableDB string
		db.QueryRow(beeorm.NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)), &skip, &createTableDB)
		createTableDB = strings.Replace(createTableDB, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", db.GetPoolConfig().GetDatabase()), 1) + ";"
		re := regexp.MustCompile(" AUTO_INCREMENT=[0-9]+ ")
		createTableDB = re.ReplaceAllString(createTableDB, " ")
		if logEntitySchema != createTableDB {
			db.QueryRow(beeorm.NewWhere("1"))
			isEmpty := !db.QueryRow(beeorm.NewWhere(fmt.Sprintf("SELECT ID FROM `%s`", tableName)))
			dropTableSQL := fmt.Sprintf("DROP TABLE `%s`.`%s`;", db.GetPoolConfig().GetDatabase(), tableName)
			sqlSchema.PostAlters = append(sqlSchema.PostAlters, beeorm.Alter{SQL: dropTableSQL, Safe: isEmpty, Pool: poolName.(string)})
			sqlSchema.PostAlters = append(sqlSchema.PostAlters, beeorm.Alter{SQL: logEntitySchema, Safe: true, Pool: poolName.(string)})
		}
	}
	return nil
}

func NewEventHandler(engine beeorm.Engine) beeorm.EventConsumerHandler {
	return func(events []beeorm.Event) {
		values := make(map[string][]*crud_stream.CrudEvent)
		for _, event := range events {
			var data crud_stream.CrudEvent
			event.Unserialize(&data)
			schema := engine.GetRegistry().GetEntitySchema(data.EntityName)
			if schema == nil {
				continue
			}
			poolName := schema.GetPluginOption(PluginCode, poolOption)
			_, has := values[poolName.(string)]
			if !has {
				values[poolName.(string)] = make([]*crud_stream.CrudEvent, 0)
			}
			values[poolName.(string)] = append(values[poolName.(string)], &data)
		}
		handleLogEvents(engine, values)
	}
}

type EntityLog struct {
	LogID    uint64
	EntityID uint64
	Date     time.Time
	MetaData beeorm.Bind
	Before   beeorm.Bind
	After    beeorm.Bind
}

func GetEntityLogs(engine beeorm.Engine, entitySchema beeorm.EntitySchema, entityID uint64, pager *beeorm.Pager, where *beeorm.Where) []EntityLog {
	var results []EntityLog
	poolName := entitySchema.GetPluginOption(PluginCode, poolOption)
	if poolName == "" {
		return results
	}
	db := engine.GetMysql(poolName.(string))
	if pager == nil {
		pager = beeorm.NewPager(1, 1000)
	}
	if where == nil {
		where = beeorm.NewWhere("1")
	}
	tableName := entitySchema.GetPluginOption(PluginCode, tableNameOption)
	fullQuery := "SELECT `id`, `added_at`, `meta`, `before`, `changes` FROM " + tableName.(string) + " WHERE "
	fullQuery += "entity_id = " + strconv.FormatUint(entityID, 10) + " "
	fullQuery += "AND " + where.String() + " " + pager.String()
	rows, closeF := db.Query(fullQuery, where.GetParameters()...)
	defer closeF()
	id := uint64(0)
	addedAt := ""
	meta := sql.NullString{}
	before := sql.NullString{}
	changes := sql.NullString{}
	for rows.Next() {
		rows.Scan(&id, &addedAt, &meta, &before, &changes)
		log := EntityLog{}
		log.LogID = id
		log.EntityID = entityID
		log.Date, _ = time.ParseInLocation(beeorm.TimeFormat, addedAt, time.Local)
		if meta.Valid {
			err := jsoniter.ConfigFastest.UnmarshalFromString(meta.String, &log.MetaData)
			if err != nil {
				panic(err)
			}
		}
		if before.Valid {
			err := jsoniter.ConfigFastest.UnmarshalFromString(before.String, &log.Before)
			if err != nil {
				panic(err)
			}
		}
		if changes.Valid {
			err := jsoniter.ConfigFastest.UnmarshalFromString(changes.String, &log.After)
			if err != nil {
				panic(err)
			}
		}
		results = append(results, log)
	}
	return results
}

func handleLogEvents(engine beeorm.Engine, values map[string][]*crud_stream.CrudEvent) {
	for poolName, rows := range values {
		poolDB := engine.GetMysql(poolName)
		if len(rows) > 1 {
			poolDB.Begin()
		}
		func() {
			defer poolDB.Rollback()
			for _, value := range rows {
				schema := engine.GetRegistry().GetEntitySchema(value.EntityName)
				tableName := schema.GetPluginOption(PluginCode, tableNameOption)
				query := "INSERT INTO `" + tableName.(string) + "`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)"
				params := make([]interface{}, 5)
				params[0] = value.ID
				params[1] = value.Updated.Format(beeorm.TimeFormat)
				meta := value.MetaData
				if len(meta) > 0 {
					params[2], _ = jsoniter.ConfigFastest.MarshalToString(meta)
				}
				if len(value.Before) > 0 {
					params[3], _ = jsoniter.ConfigFastest.MarshalToString(value.Before)
				}
				if len(value.Changes) > 0 {
					params[4], _ = jsoniter.ConfigFastest.MarshalToString(value.Changes)
				}
				func() {
					defer func() {
						if rec := recover(); rec != nil {
							asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
							if isMySQLError && asMySQLError.Number == 1146 { // table was removed
								return
							}
							panic(rec)
						}
					}()
					poolDB.Exec(query, params...)
				}()
			}
			if poolDB.IsInTransaction() {
				poolDB.Commit()
			}
		}()
	}
}
