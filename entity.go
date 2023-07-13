package beeorm

type ID interface {
	int | uint8 | uint16 | uint32 | uint64
}
