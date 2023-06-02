package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type uuidEntity struct {
	ORM  `orm:"uuid;localCache;redisCache"`
	ID   uint64
	Name string `orm:"unique=name;required"`
	Age  int
}

type uuidReferenceEntity struct {
	ORM    `orm:"uuid;localCache;redisCache"`
	ID     uint64
	Parent *uuidEntity
	Name   string `orm:"unique=name;required"`
	Size   int
}

type uuidEntityInvalid struct {
	ORM `orm:"uuid"`
	ID  uint
}

func TestUUIDdNoCache(t *testing.T) {
	testUUID(t, false, false)
}

func TestUUIDLocalCache(t *testing.T) {
	testUUID(t, true, false)
}

func TestUUIDRedisCache(t *testing.T) {
	testUUID(t, false, true)
}

func TestUUIDLocalRedisCache(t *testing.T) {
	testUUID(t, true, true)
}

func TestUUIDInvalidSchema(t *testing.T) {
	registry := &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterEntity(&uuidEntityInvalid{})
	_, err := registry.Validate()
	assert.EqualError(t, err, "entity beeorm.uuidEntityInvalid with uuid enabled must be unit64")
}

func TestUUIDServerID(t *testing.T) {
	id := uuid()
	SetUUIDServerID(1)
	id2 := uuid()
	assert.Equal(t, uint64(72057594037927937), id2-id)
}

func testUUID(t *testing.T, local bool, redis bool) {
	registry := &Registry{}
	var entity *uuidEntity
	var referenceEntity *uuidReferenceEntity
	engine := prepareTables(t, registry, 8, 6, "", entity, referenceEntity)
	engine.GetMysql().Query("DROP TABLE `uuidReferenceEntity`")
	engine.GetMysql().Query("DROP TABLE `uuidEntity`")
	alters := engine.GetAlters()
	assert.Len(t, alters, 2)
	assert.Equal(t, "CREATE TABLE `test`.`uuidEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Age` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`uuidReferenceEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Parent` bigint unsigned DEFAULT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Size` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	alters[0].Exec()
	alters[1].Exec()

	schema := engine.registry.GetTableSchemaForEntity(entity).(*tableSchema)
	schema2 := engine.registry.GetTableSchemaForEntity(referenceEntity).(*tableSchema)
	if !local {
		schema.hasLocalCache = false
		schema.localCacheName = ""
		schema2.hasLocalCache = false
		schema2.localCacheName = ""
	}
	if !redis {
		schema.hasRedisCache = false
		schema.redisCacheName = ""
		schema2.hasRedisCache = false
		schema2.redisCacheName = ""
	}

	assert.True(t, schema.hasUUID)
	assert.True(t, schema2.hasUUID)

	id := uuid()
	assert.Greater(t, id, uint64(0))
	id++
	assert.Equal(t, id, uuid())

	entity = &uuidEntity{}
	entity.Name = "test"
	entity.Age = 18
	engine.Flush(entity)
	id++
	assert.Equal(t, id, entity.ID)
	entity = &uuidEntity{}
	assert.True(t, engine.LoadByID(id, entity))
	assert.Equal(t, "test", entity.Name)
	assert.Equal(t, 18, entity.Age)

	referenceEntity = &uuidReferenceEntity{}
	referenceEntity.Name = "test reference"
	referenceEntity.Size = 40
	referenceEntity.Parent = entity
	engine.Flush(referenceEntity)
	id++
	assert.Equal(t, id, referenceEntity.ID)
	referenceEntity = &uuidReferenceEntity{}
	assert.True(t, engine.LoadByID(id, referenceEntity))
	assert.Equal(t, "test reference", referenceEntity.Name)
	assert.Equal(t, entity.ID, referenceEntity.Parent.ID)

	referenceEntity = &uuidReferenceEntity{}
	referenceEntity.Name = "test reference 2"
	referenceEntity.Parent = &uuidEntity{Name: "Name 3", Age: 10}
	engine.Flush(referenceEntity)

	id++
	referenceEntity = &uuidReferenceEntity{}
	assert.True(t, engine.LoadByID(id+1, referenceEntity))
	assert.Equal(t, "test reference 2", referenceEntity.Name)
	entity = &uuidEntity{}
	assert.True(t, engine.LoadByID(id, entity))
	assert.Equal(t, "Name 3", entity.Name)

	id += 2
	entity = &uuidEntity{}
	entity.Name = "test lazy"
	entity.Age = 33
	engine.FlushLazy(entity)
	assert.Equal(t, id, entity.ID)
	entity = &uuidEntity{}
	if local || redis {
		assert.True(t, engine.LoadByID(id, entity))
		assert.Equal(t, "test lazy", entity.Name)
	} else {
		assert.False(t, engine.LoadByID(id, entity))
	}
	engine.GetRedis().FlushAll()
	engine.GetLocalCache().Clear()
	assert.False(t, engine.LoadByID(id, entity))
}
