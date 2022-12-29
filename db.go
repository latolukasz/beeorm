package beeorm

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/pkg/errors"
)

type MySQLPoolConfig interface {
	GetCode() string
	GetDatabase() string
	GetDataSourceURI() string
	GetVersion() int
	getClient() *sql.DB
	getAutoincrement() uint64
	getMaxConnections() int
}

type mySQLPoolConfig struct {
	dataSourceName string
	code           string
	databaseName   string
	client         *sql.DB
	autoincrement  uint64
	version        int
	maxConnections int
}

func (p *mySQLPoolConfig) GetCode() string {
	return p.code
}

func (p *mySQLPoolConfig) GetDatabase() string {
	return p.databaseName
}

func (p *mySQLPoolConfig) GetDataSourceURI() string {
	return p.dataSourceName
}

func (p *mySQLPoolConfig) GetVersion() int {
	return p.version
}

func (p *mySQLPoolConfig) getClient() *sql.DB {
	return p.client
}

func (p *mySQLPoolConfig) getAutoincrement() uint64 {
	return p.autoincrement
}

func (p *mySQLPoolConfig) getMaxConnections() int {
	return p.maxConnections
}

type ExecResult interface {
	LastInsertId() uint64
	RowsAffected() uint64
}

type execResult struct {
	r sql.Result
}

func (e *execResult) LastInsertId() uint64 {
	id, err := e.r.LastInsertId()
	checkError(err)
	return uint64(id)
}

func (e *execResult) RowsAffected() uint64 {
	id, err := e.r.RowsAffected()
	checkError(err)
	return uint64(id)
}

type sqlClient interface {
	Begin() error
	Commit() error
	Rollback() (bool, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) SQLRow
	QueryRowContext(ctx context.Context, query string, args ...interface{}) SQLRow
	Query(query string, args ...interface{}) (SQLRows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (SQLRows, error)
}

type dbClientQuery interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type dbClient interface {
	dbClientQuery
	Begin() (*sql.Tx, error)
}

type dbClientTX interface {
	dbClientQuery
	Commit() error
	Rollback() error
}

type standardSQLClient struct {
	db dbClient
	tx dbClientTX
}

func (db *standardSQLClient) Begin() error {
	if db.tx != nil {
		return errors.New("transaction already started")
	}
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	db.tx = tx
	return nil
}

func (db *standardSQLClient) Commit() error {
	if db.tx == nil {
		return errors.New("transaction not started")
	}
	err := db.tx.Commit()
	if err != nil {
		return err
	}
	db.tx = nil
	return nil
}

func (db *standardSQLClient) Rollback() (bool, error) {
	if db.tx == nil {
		return false, nil
	}
	err := db.tx.Rollback()
	if err != nil {
		return true, err
	}
	db.tx = nil
	return true, nil
}

func (db *standardSQLClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db.tx != nil {
		res, err := db.tx.Exec(query, args...)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	res, err := db.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) ExecContext(context context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.tx != nil {
		res, err := db.tx.ExecContext(context, query, args...)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	res, err := db.db.ExecContext(context, query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) QueryRow(query string, args ...interface{}) SQLRow {
	if db.tx != nil {
		return db.tx.QueryRow(query, args...)
	}
	return db.db.QueryRow(query, args...)
}

func (db *standardSQLClient) QueryRowContext(ctx context.Context, query string, args ...interface{}) SQLRow {
	if db.tx != nil {
		return db.tx.QueryRowContext(ctx, query, args...)
	}
	return db.db.QueryRowContext(ctx, query, args...)
}

func (db *standardSQLClient) Query(query string, args ...interface{}) (SQLRows, error) {
	if db.tx != nil {
		rows, err := db.tx.Query(query, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	rows, err := db.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *standardSQLClient) QueryContext(ctx context.Context, query string, args ...interface{}) (SQLRows, error) {
	if db.tx != nil {
		rows, err := db.tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type SQLRows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...interface{}) error
	Columns() ([]string, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...interface{})
	Columns() []string
}

type rowsStruct struct {
	sqlRows SQLRows
}

func (r *rowsStruct) Next() bool {
	return r.sqlRows.Next()
}

func (r *rowsStruct) Columns() []string {
	columns, err := r.sqlRows.Columns()
	checkError(err)
	return columns
}

func (r *rowsStruct) Scan(dest ...interface{}) {
	err := r.sqlRows.Scan(dest...)
	checkError(err)
}

type SQLRow interface {
	Scan(dest ...interface{}) error
}

type DB struct {
	engine        *engineImplementation
	client        sqlClient
	config        MySQLPoolConfig
	inTransaction bool
}

func (db *DB) GetPoolConfig() MySQLPoolConfig {
	return db.config
}

func (db *DB) IsInTransaction() bool {
	return db.inTransaction
}

func (db *DB) Begin() {
	start := getNow(db.engine.hasDBLogger)
	err := db.client.Begin()
	if db.engine.hasDBLogger {
		db.fillLogFields("BEGIN", "START TRANSACTION", start, err)
	}
	checkError(err)
	db.inTransaction = true
}

func (db *DB) Commit() {
	start := getNow(db.engine.hasDBLogger)
	err := db.client.Commit()
	if db.engine.hasDBLogger {
		db.fillLogFields("COMMIT", "COMMIT", start, err)
	}
	checkError(err)
	db.inTransaction = false
	if db.engine.afterCommitLocalCacheSets != nil {
		for cacheCode, pairs := range db.engine.afterCommitLocalCacheSets {
			cache := db.engine.GetLocalCache(cacheCode)
			cache.MSet(pairs...)
		}
		db.engine.afterCommitLocalCacheSets = nil
	}

	if db.engine.afterCommitRedisFlusher != nil {
		db.engine.afterCommitRedisFlusher.Flush()
		db.engine.afterCommitRedisFlusher = nil
	}
}

func (db *DB) Rollback() {
	start := getNow(db.engine.hasDBLogger)
	has, err := db.client.Rollback()
	if has {
		if db.engine.hasDBLogger {
			db.fillLogFields("ROLLBACK", "ROLLBACK", start, err)
		}
	}
	checkError(err)
	db.engine.afterCommitLocalCacheSets = nil
	db.engine.afterCommitRedisFlusher = nil
	db.inTransaction = false
}

func (db *DB) Exec(query string, args ...interface{}) ExecResult {
	results, err := db.exec(query, args...)
	if err != nil {
		panic(db.convertToError(err))
	}
	return results
}

func (db *DB) exec(query string, args ...interface{}) (ExecResult, error) {
	start := getNow(db.engine.hasDBLogger)
	if db.engine.queryTimeLimit > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(db.engine.queryTimeLimit)*time.Second)
		defer cancel()
		rows, err := db.client.ExecContext(ctx, query, args...)
		if db.engine.hasDBLogger {
			message := query
			if len(args) > 0 {
				message += " " + fmt.Sprintf("%v", args)
			}
			db.fillLogFields("EXEC", message, start, err)
		}
		if err != nil {
			_, isTimeout := ctx.Deadline()
			if isTimeout {
				return nil, &mysql.MySQLError{Number: 1969, Message: fmt.Sprintf("query exceeded limit of %d seconds", db.engine.queryTimeLimit)}
			}
			return nil, err
		}
		return &execResult{r: rows}, nil
	}
	rows, err := db.client.Exec(query, args...)
	if db.engine.hasDBLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		db.fillLogFields("EXEC", message, start, err)
	}
	return &execResult{r: rows}, err
}

func (db *DB) QueryRow(query *Where, toFill ...interface{}) (found bool) {
	start := getNow(db.engine.hasDBLogger)
	if db.engine.queryTimeLimit > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(db.engine.queryTimeLimit)*time.Second)
		defer cancel()
		row := db.client.QueryRowContext(ctx, query.String(), query.GetParameters()...)
		err := row.Scan(toFill...)
		message := ""
		if db.engine.hasDBLogger {
			message = query.String()
			if len(query.GetParameters()) > 0 {
				message += " " + fmt.Sprintf("%v", query.GetParameters())
			}
		}
		if err != nil {
			_, isTimeout := ctx.Deadline()
			if isTimeout {
				panic(errors.Errorf("query exceeded limit of %d seconds", db.engine.queryTimeLimit))
			}
			if err.Error() == "sql: no rows in result set" {
				if db.engine.hasDBLogger {
					db.fillLogFields("SELECT", message, start, nil)
				}
				return false
			}
			if db.engine.hasDBLogger {
				db.fillLogFields("SELECT", message, start, err)
			}
			panic(err)
		}
		if db.engine.hasDBLogger {
			db.fillLogFields("SELECT", message, start, nil)
		}
		return true
	}
	row := db.client.QueryRow(query.String(), query.GetParameters()...)
	err := row.Scan(toFill...)
	message := ""
	if db.engine.hasDBLogger {
		message = query.String()
		if len(query.GetParameters()) > 0 {
			message += " " + fmt.Sprintf("%v", query.GetParameters())
		}
	}
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if db.engine.hasDBLogger {
				db.fillLogFields("SELECT", message, start, nil)
			}
			return false
		}
		if db.engine.hasDBLogger {
			db.fillLogFields("SELECT", message, start, err)
		}
		panic(err)
	}
	if db.engine.hasDBLogger {
		db.fillLogFields("SELECT", message, start, nil)
	}
	return true
}

func (db *DB) Query(query string, args ...interface{}) (rows Rows, close func()) {
	start := getNow(db.engine.hasDBLogger)
	if db.engine.queryTimeLimit > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(db.engine.queryTimeLimit)*time.Second)
		defer cancel()
		result, err := db.client.QueryContext(ctx, query, args...)
		if db.engine.hasDBLogger {
			message := query
			if len(args) > 0 {
				message += " " + fmt.Sprintf("%v", args)
			}
			db.fillLogFields("SELECT", message, start, err)
		}
		if err != nil {
			_, isTimeout := ctx.Deadline()
			if isTimeout {
				panic(errors.Errorf("query exceeded limit of %d seconds", db.engine.queryTimeLimit))
			}
		}
		checkError(err)
		return &rowsStruct{result}, func() {
			if result != nil {
				err := result.Err()
				checkError(err)
				err = result.Close()
				checkError(err)
			}
		}
	}
	result, err := db.client.Query(query, args...)
	if db.engine.hasDBLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		db.fillLogFields("SELECT", message, start, err)
	}
	checkError(err)
	return &rowsStruct{result}, func() {
		if result != nil {
			err := result.Err()
			checkError(err)
			err = result.Close()
			checkError(err)
		}
	}
}

func (db *DB) fillLogFields(operation, query string, start *time.Time, err error) {
	query = strings.ReplaceAll(query, "\n", " ")
	fillLogFields(db.engine.queryLoggersDB, db.GetPoolConfig().GetCode(), sourceMySQL, operation, query, start, false, err)
}

func (db *DB) convertToError(err error) error {
	sqlErr, yes := err.(*mysql.MySQLError)
	if yes {
		if sqlErr.Number == 1062 {
			var abortLabelReg, _ = regexp.Compile(` for key '(.*?)'`)
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &DuplicatedKeyError{Message: sqlErr.Message, Index: labels[1]}
			}
		} else if sqlErr.Number == 1451 || sqlErr.Number == 1452 {
			var abortLabelReg, _ = regexp.Compile(" CONSTRAINT `(.*?)`")
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &ForeignKeyError{Message: "foreign key error in key `" + labels[1] + "`", Constraint: labels[1]}
			}
		}
	}
	return err
}

func escapeSQLString(val string) string {
	dest := make([]byte, 0, 2*len(val))
	var escape byte
	for i := 0; i < len(val); i++ {
		c := val[i]
		escape = 0
		switch c {
		case 0:
			escape = '0'
		case '\n':
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"':
			escape = '"'
		case '\032':
			escape = 'Z'
		}
		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}
	return "'" + string(dest) + "'"
}

func escapeSQLValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	asString, isString := val.(string)
	if isString {
		return escapeSQLString(asString)
	}
	asTime, isTime := val.(time.Time)
	if isTime {
		return "'" + asTime.Format(timeFormat) + "'"
	}
	asTimePointer, isTimePointer := val.(*time.Time)
	if isTimePointer {
		return "'" + asTimePointer.Format(timeFormat) + "'"
	}
	asString = fmt.Sprintf("%v", val)
	if asString == "<nil>" {
		return "NULL"
	}
	return asString
}
