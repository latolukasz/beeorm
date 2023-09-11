package beeorm

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
