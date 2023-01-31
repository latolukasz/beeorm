package test

import (
	"testing"

	"github.com/latolukasz/beeorm"

	"github.com/stretchr/testify/assert"
)

func TestWhere(t *testing.T) {
	where := beeorm.NewWhere("1 AND Field = ? AND Field2 IN ?", 2, []string{"a", "b"})
	assert.Equal(t, "1 AND Field = ? AND Field2 IN (?,?)", where.String())
	assert.Equal(t, []interface{}{2, "a", "b"}, where.GetParameters())
	where.Append("AND Field3 = ? AND Field4 IN ?", "c", []string{"d", "e"})
	assert.Equal(t, "1 AND Field = ? AND Field2 IN (?,?) AND Field3 = ? AND Field4 IN (?,?)", where.String())
	assert.Equal(t, []interface{}{2, "a", "b", "c", "d", "e"}, where.GetParameters())
	where.SetParameter(3, "b2")
	assert.Equal(t, []interface{}{2, "a", "b2", "c", "d", "e"}, where.GetParameters())
	where.SetParameters("c", "d", "e")
	assert.Equal(t, []interface{}{"c", "d", "e"}, where.GetParameters())
}
