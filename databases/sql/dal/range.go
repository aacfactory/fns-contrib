package dal

func NewRange(offset int, limit int) *Range {
	if offset < 0 {
		offset = 0
	}
	if limit < 1 {
		limit = 10
	}
	return &Range{
		offset: offset,
		limit:  limit,
	}
}

type Range struct {
	offset int
	limit  int
}

func (rng *Range) Value() (offset int, limit int) {
	offset, limit = rng.offset, rng.limit
	return
}

func (rng *Range) MapToPager() (pager *Pager) {
	pager = NewPager((rng.offset/rng.limit)+1, rng.limit)
	return
}

func NewPager(no int, size int) *Pager {
	if no < 1 {
		no = 1
	}
	if size < 1 {
		size = 10
	}
	return &Pager{
		no:   no,
		size: size,
	}
}

type Pager struct {
	no   int
	size int
}

func (p *Pager) Value() (no int, size int) {
	no, size = p.no, p.size
	return
}

func (p *Pager) MapToRange() (rng *Range) {
	rng = NewRange((p.no-1)*p.size, p.size)
	return
}
