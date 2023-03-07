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

func (rng *Range) MapToPageRequest() (pager *PageRequest) {
	pager = NewPageRequest((rng.offset/rng.limit)+1, rng.limit)
	return
}

func NewPageRequest(no int, size int) *PageRequest {
	if no < 1 {
		no = 1
	}
	if size < 1 {
		size = 10
	}
	return &PageRequest{
		no:   no,
		size: size,
	}
}

type PageRequest struct {
	no   int
	size int
}

func (p *PageRequest) Value() (no int, size int) {
	no, size = p.no, p.size
	return
}

func (p *PageRequest) MapToRange() (rng *Range) {
	rng = NewRange((p.no-1)*p.size, p.size)
	return
}
