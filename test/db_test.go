package test

import (
	"database/sql"
	"testing"

	"github.com/latolukasz/beeorm/v2"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

type dbEntity struct {
	beeorm.ORM
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
	engine := PrepareTables(t, &beeorm.Registry{}, 5, 6, "", entity)
	logger := &MockLogHandler{}
	engine.RegisterQueryLogger(logger, true, false, false)
	testQueryLog := &MockLogHandler{}
	engine.RegisterQueryLogger(testQueryLog, true, false, false)

	db := engine.GetMysql()
	row := db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 1, "Tom")
	assert.Equal(t, uint64(1), row.LastInsertId())
	assert.Equal(t, uint64(1), row.RowsAffected())

	var id uint64
	var name string
	found := db.QueryRow(beeorm.NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 1), &id, &name)
	assert.True(t, found)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)

	assert.False(t, db.IsInTransaction())
	db.Begin()
	assert.True(t, db.IsInTransaction())
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	db.Rollback()
	assert.False(t, db.IsInTransaction())
	db.Rollback()
	found = db.QueryRow(beeorm.NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.False(t, found)

	db.Begin()
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	found = db.QueryRow(beeorm.NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
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

	assert.Equal(t, "default", db.GetPoolConfig().GetCode())
	assert.Equal(t, "test", db.GetPoolConfig().GetDatabase())
}

func TestDBErrors(t *testing.T) {
	var entity *dbEntity
	engine := PrepareTables(t, &beeorm.Registry{}, 5, 6, "", entity)
	db := engine.GetMysql()
	logger := &MockLogHandler{}
	engine.RegisterQueryLogger(logger, true, false, false)

	assert.PanicsWithError(t, "transaction not started", func() {
		db.Commit()
	})
	db.Begin()
	assert.PanicsWithError(t, "transaction already started", func() {
		db.Begin()
	})
	db.Commit()

	mock := &MockDBClient{OriginDB: db.GetDBClient()}
	db.SetMockDBClient(mock)
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
	mock.TX = db.GetDBClientTX()
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Commit()
	})
	mock.RollbackMock = func() error {
		return errors.Errorf("test error")
	}
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Rollback()
	})

	db.SetMockClientTX(mock)
	mock.ExecMock = func(query string, args ...interface{}) (sql.Result, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Exec("")
	})
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Exec("")
	})

	mock.QueryMock = func(query string, args ...interface{}) (*sql.Rows, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Query("")
	})
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Query("")
	})

	assert.PanicsWithError(t, "Error 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'INVALID QUERY' at line 1", func() {
		db.QueryRow(beeorm.NewWhere("INVALID QUERY"))
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
