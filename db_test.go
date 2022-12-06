package beeorm

import (
	"database/sql"
	"io"
	"log"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

type dbEntity struct {
	ORM
	ID   uint
	Name string
}

type resultMock struct {
}

func (r *resultMock) LastInsertId() (int64, error) {
	return 0, errors.New("test error")
}

func (r *resultMock) RowsAffected() (int64, error) {
	return 0, errors.New("test error")
}

func TestDB(t *testing.T) {
	var entity *dbEntity
	engine, def := prepareTables(t, &Registry{}, 5, "", entity)
	defer def()
	logger := &testLogHandler{}
	engine.RegisterQueryLogger(logger, true, false, false)
	testQueryLog := &defaultLogLogger{maxPoolLen: 0, logger: log.New(io.Discard, "", 0)}
	engine.RegisterQueryLogger(testQueryLog, true, false, false)

	db := engine.GetMysql()
	row := db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 1, "Tom")
	assert.Equal(t, uint64(1), row.LastInsertId())
	assert.Equal(t, uint64(1), row.RowsAffected())

	engine.SetQueryTimeLimit(1)
	assert.PanicsWithError(t, "query exceeded limit of 1 seconds", func() {
		db.Exec("SELECT SLEEP(5)")
	})
	engine.SetQueryTimeLimit(0)

	var id uint64
	var name string
	found := db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 1), &id, &name)
	assert.True(t, found)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)

	found = db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.False(t, found)
	engine.SetQueryTimeLimit(1)
	assert.PanicsWithError(t, "query exceeded limit of 1 seconds", func() {
		db.QueryRow(NewWhere("SELECT SLEEP(5)"))
	})
	engine.SetQueryTimeLimit(0)

	assert.False(t, db.IsInTransaction())
	db.Begin()
	assert.True(t, db.IsInTransaction())
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	db.Rollback()
	assert.False(t, db.IsInTransaction())
	db.Rollback()
	found = db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.False(t, found)

	db.Begin()
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	found = db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.True(t, found)
	rows, def := db.Query("SELECT * FROM `dbEntity` WHERE `ID` > ? ORDER BY `ID`", 0)
	assert.True(t, rows.Next())
	assert.True(t, rows.Next())
	def()
	db.Commit()
	assert.False(t, db.IsInTransaction())

	rows, def = db.Query("SELECT * FROM `dbEntity` WHERE `ID` > ? ORDER BY `ID`", 0)
	assert.Equal(t, []string{"ID", "Name"}, rows.Columns())
	assert.True(t, rows.Next())
	rows.Scan(&id, &name)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)
	assert.True(t, rows.Next())
	rows.Scan(&id, &name)
	assert.Equal(t, uint64(2), id)
	assert.Equal(t, "John", name)
	def()

	engine.SetQueryTimeLimit(1)
	assert.PanicsWithError(t, "query exceeded limit of 1 seconds", func() {
		db.Query("SELECT SLEEP(3)")
	})
	engine.SetQueryTimeLimit(0)

	assert.Equal(t, "default", db.GetPoolConfig().GetCode())
	assert.Equal(t, "test", db.GetPoolConfig().GetDatabase())

	value := []byte{0, '\n', '\r', '\\', '\'', '"', '\032'}
	assert.Equal(t, "'\\0\\n\\r\\\\\\'\\\"\\Z'", escapeSQLString(string(value)))
}

func TestDBErrors(t *testing.T) {
	var entity *dbEntity
	engine, def := prepareTables(t, &Registry{}, 5, "", entity)
	defer def()
	db := engine.GetMysql()
	logger := &testLogHandler{}
	engine.RegisterQueryLogger(logger, true, false, false)

	assert.PanicsWithError(t, "transaction not started", func() {
		db.Commit()
	})
	db.Begin()
	assert.PanicsWithError(t, "transaction already started", func() {
		db.Begin()
	})
	db.Commit()

	parent := db.client.(*standardSQLClient)
	mock := &mockDBClient{db: parent.db}
	parent.db = mock
	mock.BeginMock = func() (*sql.Tx, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Begin()
	})

	mock.BeginMock = nil
	mock.CommitMock = func() error {
		return errors.Errorf("test error")
	}
	db.Begin()
	parent.tx = mock
	assert.PanicsWithError(t, "test error", func() {
		db.Commit()
	})
	mock.RollbackMock = func() error {
		return errors.Errorf("test error")
	}
	parent.tx = mock
	assert.PanicsWithError(t, "test error", func() {
		db.Rollback()
	})

	parent.tx = nil
	mock.ExecMock = func(query string, args ...interface{}) (sql.Result, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Exec("")
	})
	parent.tx = mock
	assert.PanicsWithError(t, "test error", func() {
		db.Exec("")
	})

	mock.QueryMock = func(query string, args ...interface{}) (*sql.Rows, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Query("")
	})
	parent.tx = nil
	assert.PanicsWithError(t, "test error", func() {
		db.Query("")
	})

	assert.PanicsWithError(t, "Error 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'INVALID QUERY' at line 1", func() {
		db.QueryRow(NewWhere("INVALID QUERY"))
	})

	mock.ExecMock = func(query string, args ...interface{}) (sql.Result, error) {
		return &resultMock{}, nil
	}
	row := db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 1, "Tom")
	assert.PanicsWithError(t, "test error", func() {
		row.LastInsertId()
	})
	assert.PanicsWithError(t, "test error", func() {
		row.RowsAffected()
	})
}
