package beeorm

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockLogHandler struct {
	Logs []map[string]any
}

func (h *MockLogHandler) Handle(_ ORM, log map[string]any) {
	h.Logs = append(h.Logs, log)
}

func (h *MockLogHandler) Clear() {
	h.Logs = nil
}

func PrepareTables(t *testing.T, registry Registry, entities ...any) (orm ORM) {
	registry.RegisterMySQL("root:root@tcp(localhost:3377)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6385", 0, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6385", 1, "second", nil)
	registry.RegisterLocalCache(DefaultPoolCode, 0)

	registry.RegisterEntity(entities...)
	engine, err := registry.Validate()
	if err != nil {
		if t != nil {
			assert.NoError(t, err)
			return nil
		}
		panic(err)
	}

	orm = engine.NewContext(context.Background())
	cacheRedis := engine.Redis(DefaultPoolCode)
	cacheRedis.FlushDB(orm)
	engine.Redis("second").FlushDB(orm)

	alters := GetAlters(orm)
	for _, alter := range alters {
		alter.Exec(orm)
	}

	for _, entity := range entities {
		schema := orm.Engine().Registry().EntitySchema(entity)
		schema.TruncateTable(orm)
		schema.UpdateSchema(orm)
		cacheLocal, has := schema.GetLocalCache()
		if has {
			cacheLocal.Clear(orm)
		}
	}
	LoadUniqueKeys(engine.NewContext(context.Background()), false)
	return orm
}

type MockDBClient struct {
	OriginDB            DBClient
	PrepareMock         func(query string) (*sql.Stmt, error)
	ExecMock            func(query string, args ...any) (sql.Result, error)
	ExecContextMock     func(context context.Context, query string, args ...any) (sql.Result, error)
	QueryRowMock        func(query string, args ...any) *sql.Row
	QueryRowContextMock func(context context.Context, query string, args ...any) *sql.Row
	QueryMock           func(query string, args ...any) (*sql.Rows, error)
	QueryContextMock    func(context context.Context, query string, args ...any) (*sql.Rows, error)
	BeginMock           func() (*sql.Tx, error)
	CommitMock          func() error
	RollbackMock        func() error
}

func (m *MockDBClient) Prepare(query string) (*sql.Stmt, error) {
	if m.PrepareMock != nil {
		return m.PrepareMock(query)
	}
	return m.OriginDB.Prepare(query)
}

func (m *MockDBClient) Exec(query string, args ...any) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecMock(query, args...)
	}
	return m.OriginDB.Exec(query, args...)
}

func (m *MockDBClient) ExecContext(context context.Context, query string, args ...any) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecContextMock(context, query, args...)
	}
	return m.OriginDB.ExecContext(context, query, args...)
}

func (m *MockDBClient) QueryRow(query string, args ...any) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowMock(query, args...)
	}
	return m.OriginDB.QueryRow(query, args...)
}

func (m *MockDBClient) QueryRowContext(context context.Context, query string, args ...any) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowContextMock(context, query, args...)
	}
	return m.OriginDB.QueryRowContext(context, query, args...)
}

func (m *MockDBClient) Query(query string, args ...any) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryMock(query, args...)
	}
	return m.OriginDB.Query(query, args...)
}

func (m *MockDBClient) QueryContext(context context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryContextMock(context, query, args...)
	}
	return m.OriginDB.QueryContext(context, query, args...)
}
