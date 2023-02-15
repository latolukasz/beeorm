package foreign_keys

import (
	"testing"

	"github.com/latolukasz/beeorm/v2"
)

type foreignKeyEntity struct {
	beeorm.ORM `orm:"fk"`
	Name       string
	MyRef      *foreignKeyReferenceEntity
	MyRef2     *foreignKeyReferenceEntity `orm:"index=TestIndex"`
	MyRefSkip  *foreignKeyReferenceEntity `orm:"fk=skip"`
}

type foreignKeyReferenceEntity struct {
	beeorm.ORM
	Name string
}

func TestForeignKeys(t *testing.T) {
	var entity *foreignKeyEntity
	var ref *foreignKeyReferenceEntity

	registry := &beeorm.Registry{}
	registry.RegisterPlugin(Init(nil))
	beeorm.PrepareTables(t, registry, 8, 7, "", entity, ref)
}
