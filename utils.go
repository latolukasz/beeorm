package beeorm

import (
	"crypto/sha256"
	"fmt"
)

const cacheNilValue = ""

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

func hashString(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}
