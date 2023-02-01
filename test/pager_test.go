package test

import (
	"testing"

	"github.com/latolukasz/beeorm/v2"

	"github.com/stretchr/testify/assert"
)

func TestPager(t *testing.T) {
	pager := beeorm.NewPager(2, 100)
	assert.Equal(t, 2, pager.GetCurrentPage())
	assert.Equal(t, 100, pager.GetPageSize())
	assert.Equal(t, "LIMIT 100,100", pager.String())
	pager.IncrementPage()
	assert.Equal(t, 3, pager.GetCurrentPage())
}
