package beeorm

type ID interface {
	uint | uint8 | uint16 | uint32 | uint64
}

type Entity interface {
	GetID() uint64
	SetID(id uint64)
}
