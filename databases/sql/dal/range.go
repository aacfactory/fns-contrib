package dal

func NewRange(offset int, limit int) *Range {
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
