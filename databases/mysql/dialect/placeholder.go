package dialect

var (
	query = []byte("?")
)

type Placeholder struct {
}

func (ph *Placeholder) Next() []byte {
	return query
}

func (ph *Placeholder) SkipCursor(_ int) {
}

func (ph *Placeholder) Current() []byte {
	return query
}
