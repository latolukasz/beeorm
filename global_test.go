package beeorm

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testLogHandler struct {
	Logs []Bind
}

func (h *testLogHandler) Handle(log Bind) {
	h.Logs = append(h.Logs, log)
}

func (h *testLogHandler) clear() {
	h.Logs = nil
}

func prepareTables(t *testing.T, registry *Registry, mySQLVersion, redisVersion int, redisNamespace string, entities ...Entity) (engine *engineImplementation) {
	if mySQLVersion == 5 {
		registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test?limit_connections=10")
		registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test_log", "log")
	} else {
		registry.RegisterMySQLPool("root:root@tcp(localhost:3312)/test")
		registry.RegisterMySQLPool("root:root@tcp(localhost:3312)/test_log", "log")
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

	registry.RegisterEntity(entities...)
	vRegistry, err := registry.Validate()
	if err != nil {
		if t != nil {
			assert.NoError(t, err)
			return nil
		}
		panic(err)
	}

	engine = vRegistry.CreateEngine().(*engineImplementation)
	if t != nil {
		assert.Equal(t, engine.GetRegistry(), vRegistry)
	}
	redisCache := engine.GetRedis()
	redisCache.FlushDB()
	redisCache = engine.GetRedis("default_queue")
	redisCache.FlushDB()
	redisSearch := engine.GetRedis("search")
	redisSearch.FlushDB()

	alters := engine.GetAlters()
	for _, alter := range alters {
		alter.Exec(engine)
	}

	engine.GetMysql().Exec("SET FOREIGN_KEY_CHECKS = 0")
	for _, entity := range entities {
		eType := reflect.TypeOf(entity)
		if eType.Kind() == reflect.Ptr {
			eType = eType.Elem()
		}
		tableSchema := vRegistry.GetTableSchema(eType.String())
		tableSchema.TruncateTable(engine)
		tableSchema.UpdateSchema(engine)
		localCache, has := tableSchema.GetLocalCache(engine)
		if has {
			localCache.Clear()
		}
	}
	engine.GetMysql().Exec("SET FOREIGN_KEY_CHECKS = 1")

	indexer := NewBackgroundConsumer(engine)
	indexer.DisableBlockMode()
	indexer.blockTime = time.Millisecond
	indexer.Digest(context.Background())

	return engine
}

type mockDBClient struct {
	db                  dbClient
	tx                  dbClientTX
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

func (m *mockDBClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecMock(query, args...)
	}
	return m.db.Exec(query, args...)
}

func (m *mockDBClient) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecContextMock(ctx, query, args...)
	}
	return m.db.ExecContext(ctx, query, args...)
}

func (m *mockDBClient) QueryRow(query string, args ...interface{}) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowMock(query, args...)
	}
	return m.db.QueryRow(query, args...)
}

func (m *mockDBClient) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowContextMock(ctx, query, args...)
	}
	return m.db.QueryRowContext(ctx, query, args...)
}

func (m *mockDBClient) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryMock(query, args...)
	}
	return m.db.Query(query, args...)
}

func (m *mockDBClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryContextMock(ctx, query, args...)
	}
	return m.db.QueryContext(ctx, query, args...)
}

func (m *mockDBClient) Begin() (*sql.Tx, error) {
	if m.BeginMock != nil {
		return m.BeginMock()
	}
	return m.db.Begin()
}

func (m *mockDBClient) Rollback() error {
	if m.RollbackMock != nil {
		return m.RollbackMock()
	}
	return m.tx.Rollback()
}

func (m *mockDBClient) Commit() error {
	if m.CommitMock != nil {
		return m.CommitMock()
	}
	return m.tx.Commit()
}
