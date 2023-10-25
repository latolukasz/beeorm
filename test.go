package beeorm

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockLogHandler struct {
	Logs []map[string]interface{}
}

func (h *MockLogHandler) Handle(_ Context, log map[string]interface{}) {
	h.Logs = append(h.Logs, log)
}

func (h *MockLogHandler) Clear() {
	h.Logs = nil
}

func PrepareTables(t *testing.T, registry Registry, entities ...any) (c Context) {
	registry.RegisterMySQL("root:root@tcp(localhost:3377)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6385", 0, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6385", 1, "second", nil)
	registry.RegisterLocalCache(DefaultPoolCode, 0)

	registry.RegisterEntity(entities...)
	engine, err := registry.Validate()
	registry.RegisterEntity(entities...)
	if err != nil {
		if t != nil {
			assert.NoError(t, err)
			return nil
		}
		panic(err)
	}

	c = engine.NewContext(context.Background())
	cacheRedis := engine.Redis(DefaultPoolCode)
	cacheRedis.FlushDB(c)
	engine.Redis("second").FlushDB(c)

	alters := GetAlters(c)
	for _, alter := range alters {
		alter.Exec(c)
	}

	for _, entity := range entities {
		schema := c.Engine().Registry().EntitySchema(entity)
		schema.TruncateTable(c)
		schema.UpdateSchema(c)
		cacheLocal, has := schema.GetLocalCache()
		if has {
			cacheLocal.Clear(c)
		}
	}
	LoadUniqueKeys(engine.NewContext(context.Background()), false)
	return c
}

type MockDBClient struct {
	OriginDB            DBClient
	PrepareMock         func(query string) (*sql.Stmt, error)
	ExecMock            func(query string, args ...interface{}) (sql.Result, error)
	ExecContextMock     func(context context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowMock        func(query string, args ...interface{}) *sql.Row
	QueryRowContextMock func(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryMock           func(query string, args ...interface{}) (*sql.Rows, error)
	QueryContextMock    func(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
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

func (m *MockDBClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecMock(query, args...)
	}
	return m.OriginDB.Exec(query, args...)
}

func (m *MockDBClient) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecContextMock(ctx, query, args...)
	}
	return m.OriginDB.ExecContext(ctx, query, args...)
}

func (m *MockDBClient) QueryRow(query string, args ...interface{}) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowMock(query, args...)
	}
	return m.OriginDB.QueryRow(query, args...)
}

func (m *MockDBClient) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowContextMock(ctx, query, args...)
	}
	return m.OriginDB.QueryRowContext(ctx, query, args...)
}

func (m *MockDBClient) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryMock(query, args...)
	}
	return m.OriginDB.Query(query, args...)
}

func (m *MockDBClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryContextMock(ctx, query, args...)
	}
	return m.OriginDB.QueryContext(ctx, query, args...)
}
