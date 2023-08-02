package uuid

import (
	"testing"

	"github.com/latolukasz/beeorm/v3"

	"github.com/stretchr/testify/assert"
)

type uuidEntity struct {
	beeorm.ORM `orm:"uuid;localCache;redisCache"`
	ID         uint64
	Name       string `orm:"unique=name;required"`
	Age        int
}

type uuidReferenceEntity struct {
	beeorm.ORM `orm:"uuid;localCache;redisCache"`
	ID         uint64
	Parent     *uuidEntity
	Name       string `orm:"unique=name;required"`
	Size       int
}

type uuidInvalidEntity struct {
	beeorm.ORM `orm:"uuid"`
	ID         uint32
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
	c := beeorm.PrepareTables(t, registry, 8, 6, "", entity, referenceEntity)
	c.Engine().DB().Query(c, "DROP TABLE `uuidReferenceEntity`")
	c.Engine().DB().Query(c, "DROP TABLE `uuidEntity`")
	alters := beeorm.GetAlters(c)
	assert.Len(t, alters, 2)
	assert.Equal(t, "CREATE TABLE `test`.`uuidEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Age` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`uuidReferenceEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Parent` bigint unsigned DEFAULT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Size` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	alters[0].Exec(c)
	alters[1].Exec(c)

	schema := beeorm.GetEntitySchema[*uuidEntity](c)
	schema2 := beeorm.GetEntitySchema[*uuidReferenceEntity](c)
	schema.DisableCache(!local, !redis)
	schema2.DisableCache(!local, !redis)

	entity = &uuidEntity{}
	entity.Name = "test"
	entity.Age = 18
	c.Flusher().Track(entity).Flush()
	id := entity.GetID()
	assert.True(t, entity.GetID() > 0)
	entity = &uuidEntity{}
	assert.NotNil(t, beeorm.GetByID[*uuidEntity](c, id))
	assert.Equal(t, "test", entity.Name)
	assert.Equal(t, 18, entity.Age)

	referenceEntity = &uuidReferenceEntity{}
	referenceEntity.Name = "test reference"
	referenceEntity.Size = 40
	referenceEntity.Parent = entity
	c.Flusher().Track(referenceEntity).Flush()
	id++
	assert.Equal(t, id, referenceEntity.GetID())
	referenceEntity = &uuidReferenceEntity{}
	assert.NotNil(t, beeorm.GetByID[*uuidEntity](c, id))
	assert.Equal(t, "test reference", referenceEntity.Name)
	assert.Equal(t, entity.GetID(), referenceEntity.Parent.GetID())

	referenceEntity = &uuidReferenceEntity{}
	referenceEntity.Name = "test reference 2"
	referenceEntity.Parent = &uuidEntity{Name: "Name 3", Age: 10}
	c.Flusher().Track(referenceEntity).Flush()

	id = referenceEntity.GetID()
	referenceEntity = &uuidReferenceEntity{}
	assert.NotNil(t, beeorm.GetByID[*uuidEntity](c, id))
	assert.Equal(t, "test reference 2", referenceEntity.Name)
	id = referenceEntity.Parent.GetID()
	entity = &uuidEntity{}
	assert.NotNil(t, beeorm.GetByID[*uuidEntity](c, id))
	assert.Equal(t, "Name 3", entity.Name)

	id++
	entity = &uuidEntity{}
	entity.Name = "test lazy"
	entity.Age = 33
	c.Flusher().Track(entity).FlushLazy()
	assert.Equal(t, id, entity.GetID())
	entity = &uuidEntity{}
	if local || redis {
		assert.NotNil(t, beeorm.GetByID[*uuidEntity](c, id))
		assert.Equal(t, "test lazy", entity.Name)
	} else {
		assert.Nil(t, beeorm.GetByID[*uuidEntity](c, id))
	}
	c.Engine().Redis().FlushAll(c)
	localCache, hasLocalCache := schema.GetLocalCache()
	if hasLocalCache {
		localCache.Clear(c)
	}
	assert.Nil(t, beeorm.GetByID[*uuidEntity](c, id))

	registry = &beeorm.Registry{}
	registry.RegisterPlugin(Init(nil))
	registry.RegisterEntity(&uuidInvalidEntity{})
	_, err := registry.Validate()
	assert.Errorf(t, err, "ID field in uuid.uuidInvalidEntity must be uint64")
}
