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
	getPoolOptions() MySQLPoolOptions
}

type mySQLPoolConfig struct {
	dataSourceName string
	code           string
	databaseName   string
	client         *sql.DB
	autoincrement  uint64
	version        int
	options        MySQLPoolOptions
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

func (p *mySQLPoolConfig) getPoolOptions() MySQLPoolOptions {
	return p.options
}

type ExecResult interface {
	LastInsertId() uint64
	RowsAffected() uint64
}

type PreparedStmt interface {
	Exec(args ...any) ExecResult
	Query(args ...any) (rows Rows, close func())
	QueryRow(args []interface{}, toFill ...interface{}) (found bool)
	Close() error
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
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) SQLRow
	QueryRowContext(ctx context.Context, query string, args ...interface{}) SQLRow
	Query(query string, args ...interface{}) (SQLRows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (SQLRows, error)
}

type DBClientQuery interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type DBClient interface {
	DBClientQuery
	Begin() (*sql.Tx, error)
}

type DBClientTX interface {
	DBClientQuery
	Commit() error
	Rollback() error
}

type standardSQLClient struct {
	db DBClient
	tx DBClientTX
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

func (db *standardSQLClient) Prepare(query string) (*sql.Stmt, error) {
	if db.tx != nil {
		res, err := db.tx.Prepare(query)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	res, err := db.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return res, nil
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

type preparedStmtStruct struct {
	stmt  *sql.Stmt
	db    *DB
	query string
}

func (p preparedStmtStruct) Exec(args ...any) ExecResult {
	start := getNow(p.db.engine.hasDBLogger)
	rows, err := p.stmt.Exec(args...)
	if p.db.engine.hasDBLogger {
		message := p.query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		p.db.fillLogFields("PREPARED EXEC", message, start, err)
	}
	checkError(err)
	return &execResult{r: rows}
}

func (p preparedStmtStruct) Query(args ...any) (rows Rows, close func()) {
	start := getNow(p.db.engine.hasDBLogger)
	result, err := p.stmt.Query(args...)
	if p.db.engine.hasDBLogger {
		message := p.query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		p.db.fillLogFields("SELECT PREPARED", message, start, err)
	}
	checkError(err)
	return &rowsStruct{result}, func() {
		if result != nil {
			err := result.Err()
			_ = result.Close()
			checkError(err)
		}
	}
}

func (p preparedStmtStruct) QueryRow(args []interface{}, toFill ...interface{}) (found bool) {
	start := getNow(p.db.engine.hasDBLogger)
	row := p.stmt.QueryRow(args...)
	err := row.Scan(toFill...)
	message := ""
	if p.db.engine.hasDBLogger {
		message = p.query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
	}
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if p.db.engine.hasDBLogger {
				p.db.fillLogFields("SELECT PREPARED", message, start, nil)
			}
			return false
		}
		if p.db.engine.hasDBLogger {
			p.db.fillLogFields("SELECT PREPARED", message, start, err)
		}
		panic(err)
	}
	if p.db.engine.hasDBLogger {
		p.db.fillLogFields("SELECT PREPARED", message, start, nil)
	}
	return true
}

func (p preparedStmtStruct) Close() error {
	return p.stmt.Close()
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

func (db *DB) GetDBClient() DBClient {
	return db.client.(*standardSQLClient).db
}

func (db *DB) GetDBClientTX() DBClientTX {
	return db.client.(*standardSQLClient).tx
}

func (db *DB) SetMockDBClient(mock DBClient) {
	db.client.(*standardSQLClient).db = mock
}

func (db *DB) SetMockClientTX(mock DBClientTX) {
	db.client.(*standardSQLClient).tx = mock
}

func (db *DB) Commit() {
	start := getNow(db.engine.hasDBLogger)
	err := db.client.Commit()
	if db.engine.hasDBLogger {
		db.fillLogFields("COMMIT", "COMMIT", start, err)
	}
	checkError(err)
	db.inTransaction = false
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
	db.inTransaction = false
}

func (db *DB) Prepare(query string) (stmt PreparedStmt, close func()) {
	start := getNow(db.engine.hasDBLogger)
	result, err := db.client.Prepare(query)
	if db.engine.hasDBLogger {
		message := query
		db.fillLogFields("PREPARE", message, start, err)
	}
	checkError(err)
	return &preparedStmtStruct{result, db, query}, func() {
		if result != nil {
			_ = result.Close()
		}
	}
}

func (db *DB) Exec(query string, args ...interface{}) ExecResult {
	results, err := db.exec(query, args...)
	checkError(err)
	return results
}

func (db *DB) exec(query string, args ...interface{}) (ExecResult, error) {
	start := getNow(db.engine.hasDBLogger)
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
			_ = result.Close()
			checkError(err)
		}
	}
}

func (db *DB) fillLogFields(operation, query string, start *time.Time, err error) {
	query = strings.ReplaceAll(query, "\n", " ")
	fillLogFields(db.engine, db.engine.queryLoggersDB, db.GetPoolConfig().GetCode(), sourceMySQL, operation, query, start, false, err)
}

func convertSQLError(err error) error {
	sqlErr, yes := err.(*mysql.MySQLError)
	if yes {
		if sqlErr.Number == 1062 {
			var abortLabelReg, _ = regexp.Compile(` for key '(.*?)'`)
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &DuplicatedKeyError{Message: sqlErr.Message, Index: labels[1]}
			}
		}
	}
	return err
}
