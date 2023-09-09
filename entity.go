package beeorm

type ID interface {
	uint | uint8 | uint16 | uint32 | uint64
}

type Entity interface {
	getORM() *ORM
	GetID() uint64
	IsLoaded() bool
}

const cacheNilValue = ""
