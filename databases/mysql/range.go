package mysql

func NewRange(offset int, limit int) *Range {
	return &Range{
		Offset: offset,
		Limit:  limit,
	}
}

type Range struct {
	Offset int
	Limit  int
}
