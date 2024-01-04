package where

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhere(t *testing.T) {
	w := New()
	w.And("id", 1)
	w.And("name", "test")
	w.And("age", []any{1, 2, 3, 4, 5})

	w.Or("id", 2)
	w.AndLike("name", "%test%")

	w.And("sex", "LIKE", "%female%")

	w.AndIn("id", []any{1, 2, 3, 4, 5})

	expected := "`id` = ? AND `name` = ? AND `age` IN (?,?,?,?,?) OR `id` = ? AND `name` LIKE ? AND `sex` LIKE ? AND `id` IN (?,?,?,?,?)"

	assert.Equal(t, expected, w.String())
	assert.Equal(t, []any{
		1, "test",
		1, 2, 3, 4, 5,
		2,
		"%test%",
		"%female%",
		1, 2, 3, 4, 5,
	}, w.GetParameters())

}
