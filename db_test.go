package beeorm

import (
	"database/sql"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

type dbEntity struct {
	ORM
	ID   uint64
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
	c := PrepareTables(t, &Registry{}, 5, 6, "", entity)
	logger := &MockLogHandler{}
	c.RegisterQueryLogger(logger, true, false, false)
	testQueryLog := &MockLogHandler{}
	c.RegisterQueryLogger(testQueryLog, true, false, false)

	db := c.Engine().DB(DefaultPoolCode)
	row := db.Exec(c, "INSERT INTO `dbEntity` VALUES(?, ?)", 1, "Tom")
	assert.Equal(t, uint64(1), row.LastInsertId())
	assert.Equal(t, uint64(1), row.RowsAffected())

	var id uint64
	var name string
	found := db.QueryRow(c, NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 1), &id, &name)
	assert.True(t, found)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)

	assert.False(t, db.IsInTransaction())
	db.Begin(c)
	assert.True(t, db.IsInTransaction())
	db.Exec(c, "INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	db.Rollback(c)
	assert.False(t, db.IsInTransaction())
	db.Rollback(c)
	found = db.QueryRow(c, NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.False(t, found)

	db.Begin(c)
	db.Exec(c, "INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	found = db.QueryRow(c, NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.True(t, found)
	rows, def := db.Query(c, "SELECT * FROM `dbEntity` WHERE `ID` > ? ORDER BY `ID`", 0)
	assert.True(t, rows.Next())
	assert.True(t, rows.Next())
	def()
	db.Commit(c)
	assert.False(t, db.IsInTransaction())

	rows, def = db.Query(c, "SELECT * FROM `dbEntity` WHERE `ID` > ? ORDER BY `ID`", 0)
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

	assert.Equal(t, DefaultPoolCode, db.GetPoolConfig().GetCode())
	assert.Equal(t, "test", db.GetPoolConfig().GetDatabase())

	preparedExec, def := db.Prepare(c, "INSERT INTO `dbEntity` VALUES(?, ?)")
	assert.NotNil(t, def)
	res := preparedExec.Exec(c, 3, "Ivona")
	assert.Equal(t, uint64(3), res.LastInsertId())
	assert.Equal(t, uint64(1), res.RowsAffected())
	res = preparedExec.Exec(c, 4, "Adam")
	assert.Equal(t, uint64(4), res.LastInsertId())
	assert.Equal(t, uint64(1), res.RowsAffected())
	def()
	preparedExec, def = db.Prepare(c, "SELECT * FROM `dbEntity` WHERE `ID` = ?")
	found = preparedExec.QueryRow(c, []interface{}{1}, &id, &name)
	assert.True(t, found)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)
	found = preparedExec.QueryRow(c, []interface{}{2}, &id, &name)
	assert.True(t, found)
	assert.Equal(t, uint64(2), id)
	assert.Equal(t, "John", name)
	def()
	preparedExec, def = db.Prepare(c, "SELECT * FROM `dbEntity` WHERE `ID` >= ?")
	rows, cl := preparedExec.Query(c, 3)
	assert.True(t, rows.Next())
	rows.Scan(&id, &name)
	assert.Equal(t, uint64(3), id)
	assert.Equal(t, "Ivona", name)
	assert.True(t, rows.Next())
	rows.Scan(&id, &name)
	assert.Equal(t, uint64(4), id)
	assert.Equal(t, "Adam", name)
	assert.False(t, rows.Next())
	cl()
	def()
}

func TestDBErrors(t *testing.T) {
	var entity *dbEntity
	c := PrepareTables(t, &Registry{}, 5, 6, "", entity)
	db := c.Engine().DB(DefaultPoolCode)
	logger := &MockLogHandler{}
	c.RegisterQueryLogger(logger, true, false, false)

	assert.PanicsWithError(t, "transaction not started", func() {
		db.Commit(c)
	})
	db.Begin(c)
	assert.PanicsWithError(t, "transaction already started", func() {
		db.Begin(c)
	})
	db.Commit(c)

	mock := &MockDBClient{OriginDB: db.GetDBClient()}
	db.SetMockDBClient(mock)
	mock.BeginMock = func() (*sql.Tx, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Begin(c)
	})

	mock.BeginMock = nil
	mock.CommitMock = func() error {
		return errors.Errorf("test error")
	}
	db.Begin(c)
	mock.TX = db.GetDBClientTX()
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Commit(c)
	})
	mock.RollbackMock = func() error {
		return errors.Errorf("test error")
	}
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Rollback(c)
	})

	db.SetMockClientTX(mock)
	mock.ExecMock = func(query string, args ...interface{}) (sql.Result, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Exec(c, "")
	})
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Exec(c, "")
	})

	mock.QueryMock = func(query string, args ...interface{}) (*sql.Rows, error) {
		return nil, errors.Errorf("test error")
	}
	assert.PanicsWithError(t, "test error", func() {
		db.Query(c, "")
	})
	db.SetMockClientTX(mock)
	assert.PanicsWithError(t, "test error", func() {
		db.Query(c, "")
	})

	assert.PanicsWithError(t, "Error 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'INVALID QUERY' at line 1", func() {
		db.QueryRow(c, NewWhere("INVALID QUERY"))
	})

	mock.ExecMock = func(query string, args ...interface{}) (sql.Result, error) {
		return &resultMock{}, nil
	}
	row := db.Exec(c, "INSERT INTO `dbEntity` VALUES(?, ?)", 1, "Tom")
	assert.PanicsWithError(t, "test error", func() {
		row.LastInsertId()
	})
	assert.PanicsWithError(t, "test error", func() {
		row.RowsAffected()
	})
}
