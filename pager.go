package beeorm

import "strconv"

type Pager struct {
	CurrentPage int
	PageSize    int
}

func NewPager(currentPage, pageSize int) *Pager {
	return &Pager{
		CurrentPage: currentPage,
		PageSize:    pageSize,
	}
}

func (pager *Pager) GetPageSize() int {
	return pager.PageSize
}

func (pager *Pager) GetCurrentPage() int {
	return pager.CurrentPage
}

func (pager *Pager) IncrementPage() {
	pager.CurrentPage++
}

func (pager *Pager) String() string {
	return "LIMIT " + strconv.Itoa((pager.CurrentPage-1)*pager.PageSize) + "," + strconv.Itoa(pager.PageSize)
}
