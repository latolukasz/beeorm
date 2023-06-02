package beeorm

import "strconv"

// Pager represents a paginated list of results.
type Pager struct {
	// CurrentPage is the current page number.
	CurrentPage int

	// PageSize is the number of items per page.
	PageSize int
}

// NewPager creates a new Pager with the given page number and page size.
func NewPager(currentPage, pageSize int) *Pager {
	return &Pager{
		CurrentPage: currentPage,
		PageSize:    pageSize,
	}
}

// GetPageSize returns the page size of the Pager.
func (pager *Pager) GetPageSize() int {
	return pager.PageSize
}

// GetCurrentPage returns the current page number of the Pager.
func (pager *Pager) GetCurrentPage() int {
	return pager.CurrentPage
}

// IncrementPage increments the current page number of the Pager.
func (pager *Pager) IncrementPage() {
	pager.CurrentPage++
}

// String returns SQL 'LIMIT X,Y'.
func (pager *Pager) String() string {
	return "LIMIT " + strconv.Itoa((pager.CurrentPage-1)*pager.PageSize) + "," + strconv.Itoa(pager.PageSize)
}
