package uuid

import (
	"testing"

	"github.com/latolukasz/beeorm/v2"

	"github.com/stretchr/testify/assert"
)

type uuidEntity struct {
	beeorm.ORM `orm:"uuid;localCache;redisCache"`
	Name       string `orm:"unique=name;required"`
	Age        int
}

type uuidReferenceEntity struct {
	beeorm.ORM `orm:"uuid;localCache;redisCache"`
	Parent     *uuidEntity
	Name       string `orm:"unique=name;required"`
	Size       int
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

func testUUID(t *testing.T, local bool, redis bool) {
	registry := &beeorm.Registry{}
	registry.RegisterPlugin(Init(nil))
	var entity *uuidEntity
	var referenceEntity *uuidReferenceEntity
	engine := beeorm.PrepareTables(t, registry, 8, 6, "", entity, referenceEntity)
	engine.GetMysql().Query("DROP TABLE `uuidReferenceEntity`")
	engine.GetMysql().Query("DROP TABLE `uuidEntity`")
	alters := engine.GetAlters()
	assert.Len(t, alters, 3)
	assert.Equal(t, "CREATE TABLE `test`.`uuidEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Age` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`uuidReferenceEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Parent` bigint unsigned DEFAULT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Size` int NOT NULL DEFAULT '0',\n  INDEX `Parent` (`Parent`),\n  UNIQUE INDEX `name` (`Name`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	alters[0].Exec(engine)
	alters[1].Exec(engine)

	schema := engine.GetRegistry().GetEntitySchemaForEntity(entity)
	schema2 := engine.GetRegistry().GetEntitySchemaForEntity(referenceEntity)
	schema.DisableCache(!local, !redis)
	schema2.DisableCache(!local, !redis)

	entity = &uuidEntity{}
	entity.Name = "test"
	entity.Age = 18
	engine.Flush(entity)
	id := entity.GetID()
	assert.True(t, entity.GetID() > 0)
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
	assert.Equal(t, id, referenceEntity.GetID())
	referenceEntity = &uuidReferenceEntity{}
	assert.True(t, engine.LoadByID(id, referenceEntity))
	assert.Equal(t, "test reference", referenceEntity.Name)
	assert.Equal(t, entity.GetID(), referenceEntity.Parent.GetID())

	referenceEntity = &uuidReferenceEntity{}
	referenceEntity.Name = "test reference 2"
	referenceEntity.Parent = &uuidEntity{Name: "Name 3", Age: 10}
	engine.Flush(referenceEntity)

	id = referenceEntity.GetID()
	referenceEntity = &uuidReferenceEntity{}
	assert.True(t, engine.LoadByID(id, referenceEntity))
	assert.Equal(t, "test reference 2", referenceEntity.Name)
	id = referenceEntity.Parent.GetID()
	entity = &uuidEntity{}
	assert.True(t, engine.LoadByID(id, entity))
	assert.Equal(t, "Name 3", entity.Name)

	id++
	entity = &uuidEntity{}
	entity.Name = "test lazy"
	entity.Age = 33
	engine.FlushLazy(entity)
	assert.Equal(t, id, entity.GetID())
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
