package beeorm

import (
	"context"
	"database/sql"
	"reflect"
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

func PrepareTables(t *testing.T, registry *Registry, mySQLVersion, redisVersion int, redisNamespace string, entities ...Entity) (c Context) {
	poolOptions := MySQLPoolOptions{}
	if mySQLVersion == 5 {
		registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test", poolOptions)
		registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test_log", poolOptions, "log")
	} else {
		registry.RegisterMySQLPool("root:root@tcp(localhost:3312)/test", poolOptions)
		registry.RegisterMySQLPool("root:root@tcp(localhost:3312)/test_log", poolOptions, "log")
	}
	if redisVersion == 6 {
		registry.RegisterRedis("localhost:6382", redisNamespace, 15)
		registry.RegisterRedis("localhost:6382", redisNamespace, 14, "default_queue")
		registry.RegisterRedis("localhost:6382", redisNamespace, 0, "search")
	} else {
		registry.RegisterRedis("localhost:6381", redisNamespace, 15)
		registry.RegisterRedis("localhost:6381", redisNamespace, 14, "default_queue")
		registry.RegisterRedis("localhost:6381", redisNamespace, 0, "search")
	}

	registry.RegisterLocalCache(1000)
	registry.RegisterLocalCache(1000, "second")

	registry.RegisterEntity(entities...)
	engine, err := registry.Validate()
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
	cacheRedis = engine.Redis("default_queue")
	cacheRedis.FlushDB(c)
	redisSearch := engine.Redis("search")
	redisSearch.FlushDB(c)

	alters := GetAlters(c)
	for _, alter := range alters {
		alter.Exec(c)
	}

	engine.DB(DefaultPoolCode).Exec(c, "SET FOREIGN_KEY_CHECKS = 0")
	for _, entity := range entities {
		eType := reflect.TypeOf(entity)
		if eType.Kind() == reflect.Ptr {
			eType = eType.Elem()
		}
		schema := c.Engine().Registry().EntitySchema(eType)
		schema.TruncateTable(c)
		schema.UpdateSchema(c)
		cacheLocal, has := schema.GetLocalCache()
		if has {
			cacheLocal.Clear(c)
		}
	}
	engine.DB(DefaultPoolCode).Exec(c, "SET FOREIGN_KEY_CHECKS = 1")
	RunLazyFlushConsumer(c, true)
	return c
}

func RunLazyFlushConsumer(c Context, garbage bool) {
	consumer := NewLazyFlushConsumer(c)
	consumer.SetBlockTime(0)
	consumer.Digest()
	if garbage {
		RunStreamGarbageCollectorConsumer(c)
	}
}

func RunStreamGarbageCollectorConsumer(c Context) {
	garbageConsumer := NewStreamGarbageCollectorConsumer(c)
	garbageConsumer.SetBlockTime(0)
	garbageConsumer.Digest()
}

type MockDBClient struct {
	OriginDB            DBClient
	TX                  DBClientTX
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

func (m *MockDBClient) Begin() (*sql.Tx, error) {
	if m.BeginMock != nil {
		return m.BeginMock()
	}
	return m.OriginDB.Begin()
}

func (m *MockDBClient) Rollback() error {
	if m.RollbackMock != nil {
		return m.RollbackMock()
	}
	return m.TX.Rollback()
}

func (m *MockDBClient) Commit() error {
	if m.CommitMock != nil {
		return m.CommitMock()
	}
	return m.TX.Commit()
}
