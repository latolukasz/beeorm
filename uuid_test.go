package beeorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type uuidEntity struct {
	ORM  `orm:"uuid"`
	ID   uint64
	Name string `orm:"unique=name;required"`
	Age  int
}

type uuidReferenceEntity struct {
	ORM    `orm:"uuid"`
	ID     uint64
	Parent *uuidEntity
	Name   string `orm:"unique=name;required"`
	Size   int
}

func TestUuid(t *testing.T) {
	registry := &Registry{}
	var entity *uuidEntity
	var referenceEntity *uuidReferenceEntity
	engine, def := prepareTables(t, registry, 8, "", entity, referenceEntity)
	defer def()
	engine.GetMysql().Query("DROP TABLE `uuidReferenceEntity`")
	engine.GetMysql().Query("DROP TABLE `uuidEntity`")
	alters := engine.GetAlters()
	assert.Len(t, alters, 2)
	assert.Equal(t, "CREATE TABLE `test`.`uuidEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Age` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`uuidReferenceEntity` (\n  `ID` bigint unsigned NOT NULL,\n  `Parent` bigint unsigned DEFAULT NULL,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Size` int NOT NULL DEFAULT '0',\n  UNIQUE INDEX `name` (`Name`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	alters[0].Exec()
	alters[1].Exec()

	schema := engine.GetRegistry().GetTableSchemaForEntity(entity)
	assert.True(t, schema.(*tableSchema).hasUUID)

	id := uuid()
	assert.Greater(t, id, uint64(0))
	id++
	assert.Equal(t, id, uuid())

	entity = &uuidEntity{}
	entity.Name = "test"
	engine.EnableQueryDebug()
	engine.Flush(entity)
	id++
	assert.Equal(t, id, entity.ID)
}
