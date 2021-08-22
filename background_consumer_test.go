package beeorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type lazyReceiverEntity struct {
	ORM          `orm:"localCache;redisCache;asyncRedisLazyFlush=default"`
	ID           uint
	Name         string `orm:"unique=name"`
	Age          uint64
	EnumNullable string `orm:"enum=beeorm.TestEnum"`
	RefOne       *lazyReceiverReference
	IndexAll     *CachedQuery `query:""`
}

type lazyReceiverReference struct {
	ORM
	ID   uint
	Name string
}

func TestBackgroundConsumer(t *testing.T) {
	var entity *lazyReceiverEntity
	var ref *lazyReceiverReference

	registry := &Registry{}
	registry.RegisterEnum("beeorm.TestEnum", []string{"a", "b", "c"})
	engine, def := prepareTables(t, registry, 5, entity, ref)
	defer def()
	engine.GetRedis().FlushDB()

	receiver := NewBackgroundConsumer(engine)
	receiver.DisableLoop()
	receiver.blockTime = time.Millisecond

	e := &lazyReceiverEntity{Name: "John", Age: 18}
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded := engine.LoadByID(1, e)
	assert.False(t, loaded)

	receiver.Digest()

	engine.GetLocalCache().Clear()
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)
	assert.Equal(t, uint64(18), e.Age)

	e.Name = "Tom"
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)

	engine.GetLocalCache().Clear()
	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	receiver.Digest()

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	e = &lazyReceiverEntity{Name: "Tom"}
	e.SetOnDuplicateKeyUpdate(map[string]interface{}{"Age": 38})
	assert.PanicsWithError(t, "lazy flush on duplicate key is not supported", func() {
		engine.FlushLazy(e)
	})

	e = &lazyReceiverEntity{Name: "Adam", RefOne: &lazyReceiverReference{Name: "Test"}}
	assert.PanicsWithError(t, "lazy flush for unsaved references is not supported", func() {
		engine.FlushLazy(e)
	})

	e = &lazyReceiverEntity{}
	engine.LoadByID(1, e)
	engine.NewFlusher().Delete(e).FlushLazy()
	receiver.Digest()
	loaded = engine.LoadByID(1, e)
	assert.False(t, loaded)

	e = &lazyReceiverEntity{ID: 100}
	engine.Flush(e)
	engine.DeleteLazy(e)
	e = &lazyReceiverEntity{}
	receiver.Digest()
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	assert.False(t, engine.LoadByID(100, e))

	receiver.Shutdown(time.Second)
}
