package foreign_keys

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/latolukasz/beeorm/v3"
)

type foreignKeyEntity struct {
	beeorm.ORM `orm:"fk"`
	ID         uint64
	Name       string
	MyRef      *foreignKeyReferenceEntity
	MyRef2     *foreignKeyReferenceEntity `orm:"index=TestIndex"`
	MyRefSkip  *foreignKeyReferenceEntity `orm:"fk=skip"`
}

type foreignKeyReferenceEntity struct {
	beeorm.ORM
	ID   uint64
	Name string
}

func TestForeignKeysMySQL5(t *testing.T) {
	testForeignKeys(t, 5)
}

func TestForeignKeysMySQL8(t *testing.T) {
	testForeignKeys(t, 8)
}

func testForeignKeys(t *testing.T, mySQLVersion int) {
	var entity *foreignKeyEntity
	var ref *foreignKeyReferenceEntity

	registry := &beeorm.Registry{}
	registry.RegisterPlugin(Init(nil))
	c := beeorm.PrepareTables(t, registry, mySQLVersion, 7, "", entity, ref)
	assert.Len(t, c.Engine().GetPlugins(), 1)
	assert.Equal(t, PluginCode, c.Engine().GetPlugins()[0])
	cDrop := beeorm.PrepareTables(t, &beeorm.Registry{}, mySQLVersion, 6, "")
	for _, alter := range beeorm.GetAlters(cDrop) {
		cDrop.Engine().GetMySQL(alter.Pool).Exec(cDrop, alter.SQL)
	}
	alters := beeorm.GetAlters(c)
	assert.Len(t, alters, 3)
	if mySQLVersion == 5 {
		assert.Equal(t, "CREATE TABLE `test`.`foreignKeyReferenceEntity` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[0].SQL)
		assert.Equal(t, "CREATE TABLE `test`.`foreignKeyEntity` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n  `MyRef` bigint(20) unsigned DEFAULT NULL,\n  `MyRef2` bigint(20) unsigned DEFAULT NULL,\n  `MyRefSkip` bigint(20) unsigned DEFAULT NULL,\n  INDEX `MyRef` (`MyRef`),\n  INDEX `TestIndex` (`MyRef2`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[1].SQL)
	} else {
		assert.Equal(t, "CREATE TABLE `test`.`foreignKeyReferenceEntity` (\n  `ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
		assert.Equal(t, "CREATE TABLE `test`.`foreignKeyEntity` (\n  `ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  `MyRef` bigint unsigned DEFAULT NULL,\n  `MyRef2` bigint unsigned DEFAULT NULL,\n  `MyRefSkip` bigint unsigned DEFAULT NULL,\n  INDEX `MyRef` (`MyRef`),\n  INDEX `TestIndex` (`MyRef2`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	}
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nADD CONSTRAINT `test:foreignKeyEntity:MyRef2` FOREIGN KEY (`MyRef2`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT,\nADD CONSTRAINT `test:foreignKeyEntity:MyRef` FOREIGN KEY (`MyRef`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT;", alters[2].SQL)

	for _, alter := range alters {
		cDrop.Engine().GetMySQL(alter.Pool).Exec(cDrop, alter.SQL)
	}

	alters = beeorm.GetAlters(c)
	assert.Len(t, alters, 0)

	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` DROP FOREIGN KEY `test:foreignKeyEntity:MyRef2`")

	alters = beeorm.GetAlters(c)
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nADD CONSTRAINT `test:foreignKeyEntity:MyRef2` FOREIGN KEY (`MyRef2`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT;", alters[0].SQL)
	alters[0].Exec(c)
	assert.Len(t, beeorm.GetAlters(c), 0)

	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` ADD CONSTRAINT `abc` FOREIGN KEY (`MyRef`) REFERENCES `foreignKeyReferenceEntity` (`ID`) ON DELETE CASCADE")
	alters = beeorm.GetAlters(c)
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nDROP FOREIGN KEY `abc`;", alters[0].SQL)
	alters[0].Exec(c)
	assert.Len(t, beeorm.GetAlters(c), 0)

	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` DROP FOREIGN KEY `test:foreignKeyEntity:MyRef2`")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` ADD CONSTRAINT `test:foreignKeyEntity:MyRef2` FOREIGN KEY (`MyRef2`) REFERENCES `foreignKeyReferenceEntity` (`ID`) ON DELETE CASCADE")
	alters = beeorm.GetAlters(c)
	assert.Len(t, alters, 2)
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nDROP FOREIGN KEY `test:foreignKeyEntity:MyRef2`;", alters[0].SQL)
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nADD CONSTRAINT `test:foreignKeyEntity:MyRef2` FOREIGN KEY (`MyRef2`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT;", alters[1].SQL)
	alters[0].Exec(c)
	alters[1].Exec(c)
	assert.Len(t, beeorm.GetAlters(c), 0)

	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` DROP FOREIGN KEY `test:foreignKeyEntity:MyRef`")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` DROP FOREIGN KEY `test:foreignKeyEntity:MyRef2`")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyReferenceEntity` CHANGE COLUMN `ID` `ID` int(10) unsigned NOT NULL AUTO_INCREMENT")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` CHANGE COLUMN `MyRef` `MyRef` int(10) unsigned DEFAULT NULL")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` CHANGE COLUMN `MyRef2` `MyRef2` int(10) unsigned DEFAULT NULL")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` ADD CONSTRAINT `test:foreignKeyEntity:MyRef` FOREIGN KEY (`MyRef`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT;")
	c.Engine().GetMySQL("").Exec(c, "ALTER TABLE `foreignKeyEntity` ADD CONSTRAINT `test:foreignKeyEntity:MyRef2` FOREIGN KEY (`MyRef2`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT;")

	alters = beeorm.GetAlters(c)
	assert.Len(t, alters, 4)
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nDROP FOREIGN KEY `test:foreignKeyEntity:MyRef2`,\nDROP FOREIGN KEY `test:foreignKeyEntity:MyRef`;", alters[0].SQL)
	if mySQLVersion == 5 {
		assert.Equal(t, "ALTER TABLE `test`.`foreignKeyReferenceEntity`\n    CHANGE COLUMN `ID` `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT;/*CHANGED FROM `ID` int(10) unsigned NOT NULL AUTO_INCREMENT*/", alters[1].SQL)
		assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\n    CHANGE COLUMN `MyRef` `MyRef` bigint(20) unsigned DEFAULT NULL AFTER `Name`,/*CHANGED FROM `MyRef` int(10) unsigned DEFAULT NULL*/\n    CHANGE COLUMN `MyRef2` `MyRef2` bigint(20) unsigned DEFAULT NULL AFTER `MyRef`;/*CHANGED FROM `MyRef2` int(10) unsigned DEFAULT NULL*/", alters[2].SQL)
	} else {
		assert.Equal(t, "ALTER TABLE `test`.`foreignKeyReferenceEntity`\n    CHANGE COLUMN `ID` `ID` bigint unsigned NOT NULL AUTO_INCREMENT;/*CHANGED FROM `ID` int unsigned NOT NULL AUTO_INCREMENT*/", alters[1].SQL)
		assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\n    CHANGE COLUMN `MyRef` `MyRef` bigint unsigned DEFAULT NULL AFTER `Name`,/*CHANGED FROM `MyRef` int unsigned DEFAULT NULL*/\n    CHANGE COLUMN `MyRef2` `MyRef2` bigint unsigned DEFAULT NULL AFTER `MyRef`;/*CHANGED FROM `MyRef2` int unsigned DEFAULT NULL*/", alters[2].SQL)
	}
	assert.Equal(t, "ALTER TABLE `test`.`foreignKeyEntity`\nADD CONSTRAINT `test:foreignKeyEntity:MyRef2` FOREIGN KEY (`MyRef2`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT,\nADD CONSTRAINT `test:foreignKeyEntity:MyRef` FOREIGN KEY (`MyRef`) REFERENCES `test`.`foreignKeyReferenceEntity` (`ID`) ON DELETE RESTRICT;", alters[3].SQL)
}
