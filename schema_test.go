package beeorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type schemaSubFields struct {
	SubName        string  `orm:"required"`
	SubAge         float32 `orm:"decimal=9,5;unsigned=false"`
	Level2Sub      schemaSubFieldsLev2
	SubRefInStruct *schemaEntityRef
}

type schemaSubFieldsLev2 struct {
	Age  uint64
	Size uint64
}

type schemaSubFieldsIndex struct {
	SubName string `orm:"index=TestSubIndex"`
}

type schemaEntityRef struct {
	ORM
	ID   uint64
	Name string `orm:"required"`
}

type schemaInvalidIndexEntity struct {
	ORM
	ID   uint64
	Name string `orm:"index=TestIndex:invalid"`
}

type schemaInvalidMaxStringEntity struct {
	ORM
	ID   uint64
	Name string `orm:"length=invalid"`
}

type schemaInvalidIDEntity struct {
	ORM
}

type schemaToDropEntity struct {
	ORM
	ID uint64
}

type schemaEntity struct {
	ORM             `orm:"localCache;log;unique=TestUniqueGlobal:Year,SubStructSubAge|TestUniqueGlobal2:Uint32"`
	ID              uint64
	Name            string `orm:"index=TestIndex;required"`
	NameNullable    string
	NameMax         string  `orm:"length=max"`
	NameMaxRequired string  `orm:"length=max;required"`
	Year            *uint16 `orm:"year"`
	Uint8           uint8
	Uint16          uint16 `orm:"index=TestIndex:2"`
	Uint32          uint32
	Uint32Medium    uint32 `orm:"mediumint"`
	YearRequired    uint16 `orm:"year"`
	Uint64          uint64
	Int8            int8
	Int16           int16
	Int32           int32 `orm:"unique=TestUniqueIndex"`
	Int32Medium     int32 `orm:"mediumint"`
	Int64           int64
	Int             int
	IntNullable     *int
	Bool            bool
	BoolNullable    *bool
	Interface       interface{}
	Float32         float32
	Float32Nullable *float32
	Float64         float64
	Time            time.Time
	TimeFull        time.Time `orm:"time"`
	TimeNull        *time.Time
	Blob            []uint8
	MediumBlob      []uint8 `orm:"mediumblob"`
	LongBlob        []uint8 `orm:"longblob"`
	SubStruct       schemaSubFields
	SubStructIndex  schemaSubFieldsIndex
	schemaSubFields
	CachedQuery    *CachedQuery
	Ignored        string `orm:"ignore"`
	NameTranslated map[string]string
	RefOne         *schemaEntityRef
	RefMany        []*schemaEntityRef
	Decimal        float32      `orm:"decimal=10,2"`
	Enum           string       `orm:"enum=beeorm.testEnum;required"`
	Set            []string     `orm:"set=beeorm.testEnum;required"`
	IndexAll       *CachedQuery `query:""`
}

func TestSchema5(t *testing.T) {
	testSchema(t, 5)
}

func TestSchema8(t *testing.T) {
	testSchema(t, 8)
}

func testSchema(t *testing.T, version int) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("beeorm.testEnum", testEnum, "b")
	registry.RegisterMySQLTable(DefaultPoolCode, "TestDropTable")
	c := PrepareTables(t, registry, version, 6, "", entity, ref)

	cDrop := PrepareTables(t, &Registry{}, version, 6, "")
	for _, alter := range GetAlters(cDrop) {
		cDrop.Engine().DB(alter.Pool).Exec(cDrop, alter.SQL)
	}
	cDrop.Engine().DB(DefaultPoolCode).
		Exec(cDrop, "DROP TABLE IF EXISTS `TestDropTable`")

	alters := GetAlters(cDrop)
	assert.Len(t, alters, 2)
	assert.True(t, alters[0].Safe)
	assert.True(t, alters[1].Safe)
	assert.Equal(t, DefaultPoolCode, alters[0].Pool)
	if version == 5 {
		assert.Equal(t, "CREATE TABLE `test`.`schemaEntityRef` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL DEFAULT '',\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[0].SQL)
		assert.Equal(t, "CREATE TABLE `test`.`schemaEntity` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL DEFAULT '',\n  `NameNullable` varchar(255) DEFAULT NULL,\n  `NameMax` mediumtext,\n  `NameMaxRequired` mediumtext NOT NULL,\n  `Year` year(4) DEFAULT NULL,\n  `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0',\n  `Uint16` smallint(5) unsigned NOT NULL DEFAULT '0',\n  `Uint32` int(10) unsigned NOT NULL DEFAULT '0',\n  `Uint32Medium` mediumint(8) unsigned NOT NULL DEFAULT '0',\n  `YearRequired` year(4) NOT NULL DEFAULT '0000',\n  `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `Int8` tinyint(4) NOT NULL DEFAULT '0',\n  `Int16` smallint(6) NOT NULL DEFAULT '0',\n  `Int32` int(11) NOT NULL DEFAULT '0',\n  `Int32Medium` mediumint(9) NOT NULL DEFAULT '0',\n  `Int64` bigint(20) NOT NULL DEFAULT '0',\n  `Int` int(11) NOT NULL DEFAULT '0',\n  `IntNullable` int(11) DEFAULT NULL,\n  `Bool` tinyint(1) NOT NULL DEFAULT '0',\n  `BoolNullable` tinyint(1) DEFAULT NULL,\n  `Interface` json DEFAULT NULL,\n  `Float32` float NOT NULL DEFAULT '0',\n  `Float32Nullable` float DEFAULT NULL,\n  `Float64` double NOT NULL DEFAULT '0',\n  `Time` date NOT NULL DEFAULT '0001-01-01',\n  `TimeFull` datetime NOT NULL DEFAULT '1000-01-01 00:00:00',\n  `TimeNull` date DEFAULT NULL,\n  `Blob` blob,\n  `MediumBlob` mediumblob,\n  `LongBlob` longblob,\n  `SubStructSubName` varchar(255) NOT NULL DEFAULT '',\n  `SubStructSubAge` decimal(9,5) NOT NULL DEFAULT '0.00000',\n  `SubStructLevel2SubAge` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `SubStructLevel2SubSize` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `SubStructSubRefInStruct` bigint(20) unsigned DEFAULT NULL,\n  `SubStructIndexSubName` varchar(255) DEFAULT NULL,\n  `SubName` varchar(255) NOT NULL DEFAULT '',\n  `SubAge` decimal(9,5) NOT NULL DEFAULT '0.00000',\n  `Level2SubAge` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `Level2SubSize` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `SubRefInStruct` bigint(20) unsigned DEFAULT NULL,\n  `NameTranslated` json DEFAULT NULL,\n  `RefOne` bigint(20) unsigned DEFAULT NULL,\n  `RefMany` json DEFAULT NULL,\n  `Decimal` decimal(10,2) NOT NULL DEFAULT '0.00',\n  `Enum` enum('a','b','c') NOT NULL DEFAULT 'b',\n  `Set` set('a','b','c') NOT NULL DEFAULT 'b',\n  INDEX `TestIndex` (`Name`,`Uint16`),\n  INDEX `TestSubIndex` (`SubStructIndexSubName`),\n  UNIQUE INDEX `TestUniqueGlobal2` (`Uint32`),\n  UNIQUE INDEX `TestUniqueGlobal` (`Year`,`SubStructSubAge`),\n  UNIQUE INDEX `TestUniqueIndex` (`Int32`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[1].SQL)
	} else {
		assert.Equal(t, "CREATE TABLE `test`.`schemaEntityRef` (\n  `ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
		assert.Equal(t, "CREATE TABLE `test`.`schemaEntity` (\n  `ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `NameNullable` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  `NameMax` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,\n  `NameMaxRequired` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,\n  `Year` year DEFAULT NULL,\n  `Uint8` tinyint unsigned NOT NULL DEFAULT '0',\n  `Uint16` smallint unsigned NOT NULL DEFAULT '0',\n  `Uint32` int unsigned NOT NULL DEFAULT '0',\n  `Uint32Medium` mediumint unsigned NOT NULL DEFAULT '0',\n  `YearRequired` year NOT NULL DEFAULT '0000',\n  `Uint64` bigint unsigned NOT NULL DEFAULT '0',\n  `Int8` tinyint NOT NULL DEFAULT '0',\n  `Int16` smallint NOT NULL DEFAULT '0',\n  `Int32` int NOT NULL DEFAULT '0',\n  `Int32Medium` mediumint NOT NULL DEFAULT '0',\n  `Int64` bigint NOT NULL DEFAULT '0',\n  `Int` int NOT NULL DEFAULT '0',\n  `IntNullable` int DEFAULT NULL,\n  `Bool` tinyint(1) NOT NULL DEFAULT '0',\n  `BoolNullable` tinyint(1) DEFAULT NULL,\n  `Interface` json DEFAULT NULL,\n  `Float32` float NOT NULL DEFAULT '0',\n  `Float32Nullable` float DEFAULT NULL,\n  `Float64` double NOT NULL DEFAULT '0',\n  `Time` date NOT NULL DEFAULT '0001-01-01',\n  `TimeFull` datetime NOT NULL DEFAULT '1000-01-01 00:00:00',\n  `TimeNull` date DEFAULT NULL,\n  `Blob` blob,\n  `MediumBlob` mediumblob,\n  `LongBlob` longblob,\n  `SubStructSubName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `SubStructSubAge` decimal(9,5) NOT NULL DEFAULT '0.00000',\n  `SubStructLevel2SubAge` bigint unsigned NOT NULL DEFAULT '0',\n  `SubStructLevel2SubSize` bigint unsigned NOT NULL DEFAULT '0',\n  `SubStructSubRefInStruct` bigint unsigned DEFAULT NULL,\n  `SubStructIndexSubName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  `SubName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `SubAge` decimal(9,5) NOT NULL DEFAULT '0.00000',\n  `Level2SubAge` bigint unsigned NOT NULL DEFAULT '0',\n  `Level2SubSize` bigint unsigned NOT NULL DEFAULT '0',\n  `SubRefInStruct` bigint unsigned DEFAULT NULL,\n  `NameTranslated` json DEFAULT NULL,\n  `RefOne` bigint unsigned DEFAULT NULL,\n  `RefMany` json DEFAULT NULL,\n  `Decimal` decimal(10,2) NOT NULL DEFAULT '0.00',\n  `Enum` enum('a','b','c') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'b',\n  `Set` set('a','b','c') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'b',\n  INDEX `TestIndex` (`Name`,`Uint16`),\n  INDEX `TestSubIndex` (`SubStructIndexSubName`),\n  UNIQUE INDEX `TestUniqueGlobal2` (`Uint32`),\n  UNIQUE INDEX `TestUniqueGlobal` (`Year`,`SubStructSubAge`),\n  UNIQUE INDEX `TestUniqueIndex` (`Int32`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	}

	for _, alter := range alters {
		cDrop.Engine().DB(alter.Pool).Exec(c, alter.SQL)
	}

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntity` ENGINE=InnoDB CHARSET=utf8")
	alters = GetAlters(c)
	c.Engine().DB(DefaultPoolCode).
		Exec(c, alters[0].SQL)

	alters = GetAlters(c)
	if version == 5 {
		assert.Len(t, alters, 1)
		assert.True(t, alters[0].Safe)
		assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[0].SQL)
		c.Engine().DB(DefaultPoolCode).
			Exec(c, alters[0].SQL)
	} else {
		assert.Len(t, alters, 0)
	}

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntity` DROP COLUMN `Name`")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	if version == 5 {
		assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD COLUMN `Name` varchar(255) NOT NULL DEFAULT '' AFTER `ID`,\n    CHANGE COLUMN `NameNullable` `NameNullable` varchar(255) DEFAULT NULL AFTER `Name`,/*CHANGED FROM `NameNullable` varchar(255) CHARACTER SET utf8 DEFAULT NULL*/\n    CHANGE COLUMN `NameMax` `NameMax` mediumtext AFTER `NameNullable`,/*CHANGED FROM `NameMax` mediumtext CHARACTER SET utf8*/\n    CHANGE COLUMN `NameMaxRequired` `NameMaxRequired` mediumtext NOT NULL AFTER `NameMax`,/*CHANGED FROM `NameMaxRequired` mediumtext CHARACTER SET utf8 NOT NULL*/\n    CHANGE COLUMN `Year` `Year` year(4) DEFAULT NULL AFTER `NameMaxRequired`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint8` `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0' AFTER `Year`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint16` `Uint16` smallint(5) unsigned NOT NULL DEFAULT '0' AFTER `Uint8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32` `Uint32` int(10) unsigned NOT NULL DEFAULT '0' AFTER `Uint16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32Medium` `Uint32Medium` mediumint(8) unsigned NOT NULL DEFAULT '0' AFTER `Uint32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `YearRequired` `YearRequired` year(4) NOT NULL DEFAULT '0000' AFTER `Uint32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint64` `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `YearRequired`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int8` `Int8` tinyint(4) NOT NULL DEFAULT '0' AFTER `Uint64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int16` `Int16` smallint(6) NOT NULL DEFAULT '0' AFTER `Int8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32` `Int32` int(11) NOT NULL DEFAULT '0' AFTER `Int16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32Medium` `Int32Medium` mediumint(9) NOT NULL DEFAULT '0' AFTER `Int32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int64` `Int64` bigint(20) NOT NULL DEFAULT '0' AFTER `Int32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int` `Int` int(11) NOT NULL DEFAULT '0' AFTER `Int64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `IntNullable` `IntNullable` int(11) DEFAULT NULL AFTER `Int`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Bool` `Bool` tinyint(1) NOT NULL DEFAULT '0' AFTER `IntNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `BoolNullable` `BoolNullable` tinyint(1) DEFAULT NULL AFTER `Bool`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Interface` `Interface` json DEFAULT NULL AFTER `BoolNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32` `Float32` float NOT NULL DEFAULT '0' AFTER `Interface`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32Nullable` `Float32Nullable` float DEFAULT NULL AFTER `Float32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float64` `Float64` double NOT NULL DEFAULT '0' AFTER `Float32Nullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Time` `Time` date NOT NULL DEFAULT '0001-01-01' AFTER `Float64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeFull` `TimeFull` datetime NOT NULL DEFAULT '1000-01-01 00:00:00' AFTER `Time`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeNull` `TimeNull` date DEFAULT NULL AFTER `TimeFull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Blob` `Blob` blob AFTER `TimeNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `MediumBlob` `MediumBlob` mediumblob AFTER `Blob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `LongBlob` `LongBlob` longblob AFTER `MediumBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructSubName` `SubStructSubName` varchar(255) NOT NULL DEFAULT '' AFTER `LongBlob`,/*CHANGED FROM `SubStructSubName` varchar(255) CHARACTER SET utf8 NOT NULL DEFAULT ''*/\n    CHANGE COLUMN `SubStructSubAge` `SubStructSubAge` decimal(9,5) NOT NULL DEFAULT '0.00000' AFTER `SubStructSubName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructLevel2SubAge` `SubStructLevel2SubAge` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `SubStructSubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructLevel2SubSize` `SubStructLevel2SubSize` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `SubStructLevel2SubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructSubRefInStruct` `SubStructSubRefInStruct` bigint(20) unsigned DEFAULT NULL AFTER `SubStructLevel2SubSize`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructIndexSubName` `SubStructIndexSubName` varchar(255) DEFAULT NULL AFTER `SubStructSubRefInStruct`,/*CHANGED FROM `SubStructIndexSubName` varchar(255) CHARACTER SET utf8 DEFAULT NULL*/\n    CHANGE COLUMN `SubName` `SubName` varchar(255) NOT NULL DEFAULT '' AFTER `SubStructIndexSubName`,/*CHANGED FROM `SubName` varchar(255) CHARACTER SET utf8 NOT NULL DEFAULT ''*/\n    CHANGE COLUMN `SubAge` `SubAge` decimal(9,5) NOT NULL DEFAULT '0.00000' AFTER `SubName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Level2SubAge` `Level2SubAge` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `SubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Level2SubSize` `Level2SubSize` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `Level2SubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubRefInStruct` `SubRefInStruct` bigint(20) unsigned DEFAULT NULL AFTER `Level2SubSize`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameTranslated` `NameTranslated` json DEFAULT NULL AFTER `SubRefInStruct`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefOne` `RefOne` bigint(20) unsigned DEFAULT NULL AFTER `NameTranslated`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefMany` `RefMany` json DEFAULT NULL AFTER `RefOne`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Decimal` `Decimal` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `RefMany`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Enum` `Enum` enum('a','b','c') NOT NULL DEFAULT 'b' AFTER `Decimal`,/*CHANGED FROM `Enum` enum('a','b','c') CHARACTER SET utf8 NOT NULL DEFAULT 'b'*/\n    CHANGE COLUMN `Set` `Set` set('a','b','c') NOT NULL DEFAULT 'b' AFTER `Enum`,/*CHANGED FROM `Set` set('a','b','c') CHARACTER SET utf8 NOT NULL DEFAULT 'b'*/\n    DROP INDEX `TestIndex`,\n    ADD INDEX `TestIndex` (`Name`,`Uint16`);", alters[0].SQL)
	} else {
		assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD COLUMN `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' AFTER `ID`,\n    CHANGE COLUMN `NameNullable` `NameNullable` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL AFTER `Name`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameMax` `NameMax` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci AFTER `NameNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameMaxRequired` `NameMaxRequired` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL AFTER `NameMax`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Year` `Year` year DEFAULT NULL AFTER `NameMaxRequired`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint8` `Uint8` tinyint unsigned NOT NULL DEFAULT '0' AFTER `Year`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint16` `Uint16` smallint unsigned NOT NULL DEFAULT '0' AFTER `Uint8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32` `Uint32` int unsigned NOT NULL DEFAULT '0' AFTER `Uint16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32Medium` `Uint32Medium` mediumint unsigned NOT NULL DEFAULT '0' AFTER `Uint32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `YearRequired` `YearRequired` year NOT NULL DEFAULT '0000' AFTER `Uint32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint64` `Uint64` bigint unsigned NOT NULL DEFAULT '0' AFTER `YearRequired`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int8` `Int8` tinyint NOT NULL DEFAULT '0' AFTER `Uint64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int16` `Int16` smallint NOT NULL DEFAULT '0' AFTER `Int8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32` `Int32` int NOT NULL DEFAULT '0' AFTER `Int16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32Medium` `Int32Medium` mediumint NOT NULL DEFAULT '0' AFTER `Int32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int64` `Int64` bigint NOT NULL DEFAULT '0' AFTER `Int32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int` `Int` int NOT NULL DEFAULT '0' AFTER `Int64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `IntNullable` `IntNullable` int DEFAULT NULL AFTER `Int`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Bool` `Bool` tinyint(1) NOT NULL DEFAULT '0' AFTER `IntNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `BoolNullable` `BoolNullable` tinyint(1) DEFAULT NULL AFTER `Bool`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Interface` `Interface` json DEFAULT NULL AFTER `BoolNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32` `Float32` float NOT NULL DEFAULT '0' AFTER `Interface`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32Nullable` `Float32Nullable` float DEFAULT NULL AFTER `Float32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float64` `Float64` double NOT NULL DEFAULT '0' AFTER `Float32Nullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Time` `Time` date NOT NULL DEFAULT '0001-01-01' AFTER `Float64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeFull` `TimeFull` datetime NOT NULL DEFAULT '1000-01-01 00:00:00' AFTER `Time`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeNull` `TimeNull` date DEFAULT NULL AFTER `TimeFull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Blob` `Blob` blob AFTER `TimeNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `MediumBlob` `MediumBlob` mediumblob AFTER `Blob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `LongBlob` `LongBlob` longblob AFTER `MediumBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructSubName` `SubStructSubName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' AFTER `LongBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructSubAge` `SubStructSubAge` decimal(9,5) NOT NULL DEFAULT '0.00000' AFTER `SubStructSubName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructLevel2SubAge` `SubStructLevel2SubAge` bigint unsigned NOT NULL DEFAULT '0' AFTER `SubStructSubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructLevel2SubSize` `SubStructLevel2SubSize` bigint unsigned NOT NULL DEFAULT '0' AFTER `SubStructLevel2SubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructSubRefInStruct` `SubStructSubRefInStruct` bigint unsigned DEFAULT NULL AFTER `SubStructLevel2SubSize`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructIndexSubName` `SubStructIndexSubName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL AFTER `SubStructSubRefInStruct`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubName` `SubName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' AFTER `SubStructIndexSubName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubAge` `SubAge` decimal(9,5) NOT NULL DEFAULT '0.00000' AFTER `SubName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Level2SubAge` `Level2SubAge` bigint unsigned NOT NULL DEFAULT '0' AFTER `SubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Level2SubSize` `Level2SubSize` bigint unsigned NOT NULL DEFAULT '0' AFTER `Level2SubAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubRefInStruct` `SubRefInStruct` bigint unsigned DEFAULT NULL AFTER `Level2SubSize`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameTranslated` `NameTranslated` json DEFAULT NULL AFTER `SubRefInStruct`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefOne` `RefOne` bigint unsigned DEFAULT NULL AFTER `NameTranslated`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefMany` `RefMany` json DEFAULT NULL AFTER `RefOne`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Decimal` `Decimal` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `RefMany`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Enum` `Enum` enum('a','b','c') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'b' AFTER `Decimal`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Set` `Set` set('a','b','c') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'b' AFTER `Enum`,/*CHANGED ORDER*/\n    DROP INDEX `TestIndex`,\n    ADD INDEX `TestIndex` (`Name`,`Uint16`);", alters[0].SQL)
	}
	c.Engine().DB(DefaultPoolCode).
		Exec(c, alters[0].SQL)

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntity` CHANGE COLUMN `Year` `Year` varchar(255) NOT NULL DEFAULT ''")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	if version == 5 {
		assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    CHANGE COLUMN `Year` `Year` year(4) DEFAULT NULL AFTER `NameMaxRequired`;/*CHANGED FROM `Year` varchar(255) NOT NULL DEFAULT ''*/", alters[0].SQL)
	} else {
		assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    CHANGE COLUMN `Year` `Year` year DEFAULT NULL AFTER `NameMaxRequired`;/*CHANGED FROM `Year` varchar(255) NOT NULL DEFAULT ''*/", alters[0].SQL)
	}
	c.Engine().DB(DefaultPoolCode).
		Exec(c, alters[0].SQL)

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntity` ADD COLUMN `Year2` varchar(255) NOT NULL DEFAULT ''")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    DROP COLUMN `Year2`;", alters[0].SQL)
	c.Engine().DB(DefaultPoolCode).
		Exec(c, alters[0].SQL)

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntity` DROP INDEX `TestIndex`")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD INDEX `TestIndex` (`Name`,`Uint16`);", alters[0].SQL)
	c.Engine().DB(DefaultPoolCode).
		Exec(c, alters[0].SQL)

	schema := GetEntitySchema[*schemaEntity](c)
	assert.Equal(t, "beeorm.schemaEntity", schema.GetType().String())
	references := schema.GetReferences()
	assert.Len(t, references, 3)
	i := 0
	for _, reference := range references {
		switch reference.ColumnName {
		case "SubStructSubRefInStruct":
			assert.Equal(t, []string{"SubStruct", "SubRefInStruct"}, reference.FieldPath)
			assert.Equal(t, "beeorm.schemaEntityRef", reference.EntityName)
			i++
		case "RefOne":
			assert.Equal(t, []string{"RefOne"}, reference.FieldPath)
			assert.Equal(t, "beeorm.schemaEntityRef", reference.EntityName)
			i++
		case "SubRefInStruct":
			assert.Equal(t, []string{"SubRefInStruct"}, reference.FieldPath)
			assert.Equal(t, "beeorm.schemaEntityRef", reference.EntityName)
			i++
		}
	}
	assert.Equal(t, 3, i)
	columns := schema.GetColumns()
	assert.Len(t, columns, 48)

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntity` ADD INDEX `TestIndex2` (`Name`);")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    DROP INDEX `TestIndex2`;", alters[0].SQL)
	schema.UpdateSchema(c)

	c.Flusher().Track(&schemaEntityRef{Name: "test"}).Flush()
	c.Engine().DB(DefaultPoolCode).
		Exec(c, "ALTER TABLE `schemaEntityRef` ADD COLUMN `Year2` varchar(255) NOT NULL DEFAULT ''")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	assert.False(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntityRef`\n    DROP COLUMN `Year2`;", alters[0].SQL)
	GetEntitySchema[*schemaEntityRef](c).UpdateSchemaAndTruncateTable(c)
	alters = GetAlters(c)
	assert.Len(t, alters, 0)

	c.Engine().DB(DefaultPoolCode).
		Exec(c, "CREATE TABLE `TestDropTable` (`field` int(11) unsigned NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
	c.Engine().DB(DefaultPoolCode).
		Exec(c, "CREATE TABLE `invalid_table` (`field` int(11) unsigned NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
	alters = GetAlters(c)
	assert.Len(t, alters, 1)
	assert.Equal(t, "DROP TABLE IF EXISTS `test`.`invalid_table`;", alters[0].SQL)

	pool := "root:root@tcp(localhost:3311)/test"
	if version == 8 {
		pool = "root:root@tcp(localhost:3312)/test"
	}

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	registry.RegisterEntity(&schemaInvalidIndexEntity{})
	_, err := registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'beeorm.schemaInvalidIndexEntity': invalid index position 'invalid' in index 'TestIndex'")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	registry.RegisterEntity(&schemaInvalidMaxStringEntity{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'beeorm.schemaInvalidMaxStringEntity': invalid max string: invalid")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	registry.RegisterEntity(&schemaInvalidIDEntity{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'beeorm.schemaInvalidIDEntity': field ID on position 1 is missing")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	registry.RegisterLocalCache(1000)
	registry.RegisterEntity(&schemaEntity{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'beeorm.schemaEntity': unregistered enum beeorm.testEnum")

	c = PrepareTables(t, &Registry{}, 5, 6, "", &schemaToDropEntity{})
	schema = GetEntitySchema[*schemaToDropEntity](c)
	schema.DropTable(c)
	has, alters := schema.GetSchemaChanges(c)
	assert.True(t, has)
	assert.Len(t, alters, 1)
	assert.Equal(t, "CREATE TABLE `test`.`schemaToDropEntity` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[0].SQL)

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema struct {
		ORM `orm:"mysql=invalid"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "mysql pool 'invalid' not found")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema2 struct {
		ORM `orm:"localCache=invalid"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema2{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "invalid local cache limit for 'beeorm.invalidSchema2'")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema3 struct {
		ORM `orm:"redisCache=invalid"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema3{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "redis pool 'invalid' not found")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{}, "other")
	type invalidSchema4 struct {
		ORM `orm:"mysql=other"`
		ID  uint64
	}
	registry.RegisterEntity(&invalidSchema4{})
	_, err = registry.Validate()
	assert.NoError(t, err)

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema5 struct {
		ORM
		Name string `orm:"index=test,test2"`
	}
	registry.RegisterEntity(&invalidSchema5{})
	_, err = registry.Validate()
	assert.NotNil(t, err)

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema6 struct {
		ORM
		Name      string
		IndexName *CachedQuery `queryOne:":Name = ?"`
	}
	registry.RegisterEntity(&invalidSchema6{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing unique index for cached query 'IndexName' in beeorm.invalidSchema6")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema7 struct {
		ORM
		Name      string
		IndexName *CachedQuery `query:":Name = ?"`
	}
	registry.RegisterEntity(&invalidSchema7{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing index for cached query 'IndexName' in beeorm.invalidSchema7")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema8 struct {
		ORM
		Name      string       `orm:"unique=TestUniqueIndex"`
		Age       uint         `orm:"unique=TestUniqueIndex:2"`
		IndexName *CachedQuery `queryOne:":Name = ?"`
	}
	registry.RegisterEntity(&invalidSchema8{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing unique index for cached query 'IndexName' in beeorm.invalidSchema8")

	registry = &Registry{}
	registry.RegisterMySQLPool(pool, MySQLPoolOptions{})
	type invalidSchema9 struct {
		ORM
		Name      string `orm:"index=TestIndex"`
		Age       uint
		IndexName *CachedQuery `query:":Name = ? ORDER BY :Age DESC"`
	}
	registry.RegisterEntity(&invalidSchema9{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing index for cached query 'IndexName' in beeorm.invalidSchema9")
}
