package fake_delete

import (
	"testing"

	"github.com/latolukasz/beeorm/v3"

	"github.com/stretchr/testify/assert"
)

type fakeDeleteEntity struct {
	beeorm.ORM
	ID         uint64
	Name       string `orm:"unique=name;required"`
	Age        int    `orm:"index=AgeWeight"`
	Weight     int    `orm:"index=AgeWeight:2"`
	FakeDelete bool
}

type noFakeDeleteEntity struct {
	beeorm.ORM
	ID   uint64
	Name string
}

func TestFakeDeleteMySQL5(t *testing.T) {
	testFakeDelete(t, 5)
}

func TestFakeDeleteMySQL8(t *testing.T) {
	testFakeDelete(t, 8)
}

func testFakeDelete(t *testing.T, mySQLVersion int) {
	registry := &beeorm.Registry{}
	registry.RegisterPlugin(Init(nil))
	var entity *fakeDeleteEntity
	var entityNoFakeDelete *noFakeDeleteEntity
	c := beeorm.PrepareTables(t, registry, mySQLVersion, 6, "", entity, entityNoFakeDelete)
	c.Engine().GetMySQL().Query(c, "DROP TABLE `fakeDeleteEntity`")
	c.Engine().GetMySQL().Query(c, "DROP TABLE `noFakeDeleteEntity`")
	alters := beeorm.GetAlters(c)
	assert.Len(t, alters, 2)
	if mySQLVersion == 5 {
		assert.Equal(t, "CREATE TABLE `test`.`noFakeDeleteEntity` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[0].SQL)
		assert.Equal(t, "CREATE TABLE `test`.`fakeDeleteEntity` (\n  `ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL DEFAULT '',\n  `Age` int(11) NOT NULL DEFAULT '0',\n  `Weight` int(11) NOT NULL DEFAULT '0',\n  `FakeDelete` bigint(20) unsigned NOT NULL,\n  INDEX `AgeWeight` (`Age`,`Weight`,`FakeDelete`),\n  INDEX `FakeDelete` (`FakeDelete`),\n  UNIQUE INDEX `name` (`Name`,`FakeDelete`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[1].SQL)
	} else {
		assert.Equal(t, "CREATE TABLE `test`.`noFakeDeleteEntity` (\n  `ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[0].SQL)
		assert.Equal(t, "CREATE TABLE `test`.`fakeDeleteEntity` (\n  `ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n  `Age` int NOT NULL DEFAULT '0',\n  `Weight` int NOT NULL DEFAULT '0',\n  `FakeDelete` bigint unsigned NOT NULL,\n  INDEX `AgeWeight` (`Age`,`Weight`,`FakeDelete`),\n  INDEX `FakeDelete` (`FakeDelete`),\n  UNIQUE INDEX `name` (`Name`,`FakeDelete`),\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", alters[1].SQL)
	}
	alters[0].Exec(c)
	alters[1].Exec(c)
	assert.Len(t, beeorm.GetAlters(c), 0)

	entity = &fakeDeleteEntity{ID: 17557, Name: "A", Age: 10, Weight: 180}
	entity2 := &fakeDeleteEntity{ID: 17558, Name: "B", Age: 20, Weight: 200}
	c.Flusher().Track(entity, entity2).Flush()

	var rows []*fakeDeleteEntity
	total := beeorm.SearchWithCount(c, beeorm.NewWhere("`FakeDelete` = 0"), nil, &rows)
	assert.Equal(t, 2, total)

	c.Flusher().Delete(entity).Flush()
	total = beeorm.SearchWithCount(c, beeorm.NewWhere("`FakeDelete` = ID"), nil, &rows)
	assert.Equal(t, 1, total)
	assert.Equal(t, "A", rows[0].Name)

	total = beeorm.SearchWithCount(c, beeorm.NewWhere("1"), nil, &rows)
	assert.Equal(t, 1, total)
	assert.Equal(t, "B", rows[0].Name)
	total = beeorm.SearchWithCount(c, beeorm.NewWhere("1 ORDER BY ID DESC"), nil, &rows)
	assert.Equal(t, 1, total)
	assert.Equal(t, "B", rows[0].Name)

	entity, found := beeorm.SearchOne[*fakeDeleteEntity](c, beeorm.NewWhere("1 ORDER BY ID ASC"))
	assert.True(t, found)
	assert.Equal(t, "B", entity.Name)
	ids := beeorm.SearchIDs[*fakeDeleteEntity](c, beeorm.NewWhere("1 ORDER BY ID ASC"), beeorm.NewPager(1, 100))
	assert.Len(t, ids, 1)
	assert.Equal(t, uint64(17558), ids[0])

	entity = beeorm.GetByID[*fakeDeleteEntity](c, 17557)
	assert.NotNil(t, entity)

	beeorm.GetByIDs(c, []uint64{17557}, &rows)

	assert.NotNil(t, beeorm.GetByID[*fakeDeleteEntity](c, 17558))
	ForceDelete(entity)
	c.Flusher().Delete(entity).Flush()
	assert.Nil(t, beeorm.GetByID[*fakeDeleteEntity](c, 17558))
}
