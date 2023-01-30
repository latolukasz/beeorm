package log_tables

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/latolukasz/beeorm"
	"regexp"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const PluginCodeLog = "beeorm/log_tables"
const LogTablesChannelName = "orm-table-log-channel"
const LogTablesConsumerGroupName = "log-tables-consumer"
const poolOption = "pool"
const tableNameOption = "table_name"
const skipLogOption = "skip-table-log"
const metaOption = "meta"

type LogTablesPlugin struct{}

func Init() *LogTablesPlugin {
	return &LogTablesPlugin{}
}

func (p *LogTablesPlugin) GetCode() string {
	return PluginCodeLog
}

func (p *LogTablesPlugin) InterfaceInitTableSchema(schema beeorm.SettableTableSchema, _ *beeorm.Registry) error {
	logPoolName := schema.GetTag("ORM", "log", "default", "")
	if logPoolName == "" {
		return nil
	}
	schema.SetOption(PluginCodeLog, poolOption, logPoolName)
	schema.SetOption(PluginCodeLog, tableNameOption, fmt.Sprintf("_log_%s_%s", logPoolName, schema.GetTableName()))
	skipLogs := make([]string, 0)
	for _, columnName := range schema.GetColumns() {
		skipLog := schema.GetTag(columnName, skipLogOption, "1", "")
		if skipLog == "1" {
			skipLogs = append(skipLogs, columnName)
		}
	}
	if len(skipLogs) > 0 {
		schema.SetOption(PluginCodeLog, skipLogOption, skipLogs)
	}
	return nil
}

func SetMetaData(engine beeorm.Engine, key string, value interface{}) {
	before := engine.GetOption(PluginCodeLog, "meta")
	if before == nil {
		engine.SetOption(PluginCodeLog, metaOption, map[string]interface{}{key: value})
	} else {
		before.(map[string]interface{})[key] = value
	}
}

func (p *LogTablesPlugin) InterfaceRegistryValidate(registry *beeorm.Registry, validatedRegistry beeorm.ValidatedRegistry) error {
	hasLog := false
	for entityName := range validatedRegistry.GetEntities() {
		poolName := validatedRegistry.GetTableSchema(entityName).GetOptionString(PluginCodeLog, poolOption)
		if poolName != "" {
			_, has := validatedRegistry.GetMySQLPools()[poolName]
			if !has {
				return fmt.Errorf("invalid log tables pool name `%s` in %s entity", poolName, entityName)
			}
			hasLog = true
		}
	}
	if hasLog {
		hasStream := false
		for _, streams := range validatedRegistry.GetRedisStreams() {
			_, hasStream = streams[LogTablesChannelName]
			if hasStream {
				break
			}
		}
		if !hasStream {
			registry.RegisterRedisStream(LogTablesChannelName, "default", []string{LogTablesConsumerGroupName})
		}
	}
	return nil
}

func (p *LogTablesPlugin) PluginInterfaceSchemaCheck(engine beeorm.Engine, schema beeorm.TableSchema) (alters []beeorm.Alter, keepTables map[string][]string) {
	poolName := schema.GetOptionString(PluginCodeLog, poolOption)
	if poolName == "" {
		return nil, nil
	}
	tableName := schema.GetOptionString(PluginCodeLog, tableNameOption)
	db := engine.GetMysql(poolName)
	var tableDef string
	hasLogTable := db.QueryRow(beeorm.NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)), &tableDef)
	var logTableSchema string
	if db.GetPoolConfig().GetVersion() == 5 {
		logTableSchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  "+
			"`entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
			"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
			db.GetPoolConfig().GetDatabase(), tableName)
	} else {
		logTableSchema = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  "+
			"`entity_id` int unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  "+
			"PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_%s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;",
			db.GetPoolConfig().GetDatabase(), tableName, engine.GetRegistry().GetSourceRegistry().GetDefaultCollate())
	}

	if !hasLogTable {
		alters = append(alters, beeorm.Alter{SQL: logTableSchema, Safe: true, Pool: poolName})
	} else {
		var skip, createTableDB string
		db.QueryRow(beeorm.NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)), &skip, &createTableDB)
		createTableDB = strings.Replace(createTableDB, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", db.GetPoolConfig().GetDatabase()), 1) + ";"
		re := regexp.MustCompile(" AUTO_INCREMENT=[0-9]+ ")
		createTableDB = re.ReplaceAllString(createTableDB, " ")
		if logTableSchema != createTableDB {
			db.QueryRow(beeorm.NewWhere("1"))
			isEmpty := !db.QueryRow(beeorm.NewWhere(fmt.Sprintf("SELECT ID FROM `%s`", tableName)))
			dropTableSQL := fmt.Sprintf("DROP TABLE `%s`.`%s`;", db.GetPoolConfig().GetDatabase(), tableName)
			alters = append(alters, beeorm.Alter{SQL: dropTableSQL, Safe: isEmpty, Pool: poolName})
			alters = append(alters, beeorm.Alter{SQL: logTableSchema, Safe: true, Pool: poolName})
		}
	}
	return alters, map[string][]string{poolName: {tableName}}
}

func (p *LogTablesPlugin) PluginInterfaceEntityFlushed(engine beeorm.Engine, flush *beeorm.EntitySQLFlush, cacheFlusher beeorm.FlusherCacheSetter) {
	tableSchema := engine.GetRegistry().GetTableSchema(flush.EntityName)
	poolName := tableSchema.GetOptionString(PluginCodeLog, poolOption)
	if poolName == "" {
		return
	}
	skippedFields := tableSchema.GetOption(PluginCodeLog, skipLogOption)
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
	val := &LogQueueValue{
		TableName: tableSchema.GetOptionString(PluginCodeLog, tableNameOption),
		ID:        flush.ID,
		PoolName:  poolName,
		Before:    flush.Old,
		Changes:   flush.Update,
		Updated:   time.Now()}
	meta := engine.GetOption(PluginCodeLog, metaOption)
	if meta != nil {
		val.Meta = meta.(map[string]interface{})
	}
	cacheFlusher.PublishToStream(LogTablesChannelName, val)
}

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	LogID     uint64
	Meta      map[string]interface{}
	Before    beeorm.Bind
	Changes   beeorm.Bind
	Updated   time.Time
}

func NewEventHandler(engine beeorm.Engine) beeorm.EventConsumerHandler {
	return func(events []beeorm.Event) {
		values := make(map[string][]*LogQueueValue)
		for _, event := range events {
			var data LogQueueValue
			event.Unserialize(&data)
			_, has := values[data.PoolName]
			if !has {
				values[data.PoolName] = make([]*LogQueueValue, 0)
			}
			values[data.PoolName] = append(values[data.PoolName], &data)
		}
		handleLogEvents(engine, values)
	}
}

type EntityLog struct {
	LogID    uint64
	EntityID uint64
	Date     time.Time
	Meta     map[string]interface{}
	Before   map[string]interface{}
	Changes  map[string]interface{}
}

func GetEntityLogs(engine beeorm.Engine, tableSchema beeorm.TableSchema, entityID uint64, pager *beeorm.Pager, where *beeorm.Where) []EntityLog {
	var results []EntityLog
	poolName := tableSchema.GetOptionString(PluginCodeLog, poolOption)
	if poolName == "" {
		return results
	}
	db := engine.GetMysql(poolName)
	if pager == nil {
		pager = beeorm.NewPager(1, 1000)
	}
	if where == nil {
		where = beeorm.NewWhere("1")
	}
	tableName := tableSchema.GetOptionString(PluginCodeLog, tableNameOption)
	fullQuery := "SELECT `id`, `added_at`, `meta`, `before`, `changes` FROM " + tableName + " WHERE "
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
		if meta.Valid {
			err := jsoniter.ConfigFastest.UnmarshalFromString(meta.String, &log.Meta)
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
			err := jsoniter.ConfigFastest.UnmarshalFromString(changes.String, &log.Changes)
			if err != nil {
				panic(err)
			}
		}
		results = append(results, log)
	}
	return results
}

func handleLogEvents(engine beeorm.Engine, values map[string][]*LogQueueValue) {
	for poolName, rows := range values {
		poolDB := engine.GetMysql(poolName)
		if len(rows) > 1 {
			poolDB.Begin()
		}
		func() {
			defer poolDB.Rollback()
			for _, value := range rows {
				/* #nosec */
				query := "INSERT INTO `" + value.TableName + "`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)"
				params := make([]interface{}, 5)
				params[0] = value.ID
				params[1] = value.Updated.Format(beeorm.TimeFormat)
				if value.Meta != nil {
					params[2], _ = jsoniter.ConfigFastest.MarshalToString(value.Meta)
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
