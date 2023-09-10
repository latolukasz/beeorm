package beeorm

type ID interface {
	uint | uint8 | uint16 | uint32 | uint64
}

type Entity interface {
	getORM() *ORM
	GetID() uint64
}

type ORM struct {
	binary []byte
	loaded bool
}

func (orm *ORM) getORM() *ORM {
	return orm
}
