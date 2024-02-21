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

func hashString(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}
