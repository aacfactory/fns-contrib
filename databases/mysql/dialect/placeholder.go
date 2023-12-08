package dialect

const (
	query = "?"
)

type Placeholder struct {
}

func (ph *Placeholder) Next() string {
	return query
}

func (ph *Placeholder) SkipCursor(_ int) {
}

func (ph *Placeholder) Current() string {
	return query
}
