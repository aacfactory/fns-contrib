package mysql

type Select interface {
	Build(args []interface{}) (query string)
}

func LitSelect(query string) Select {
	return &litSelect{
		query: query,
	}
}

type litSelect struct {
	query string
}

func (s *litSelect) Build(_ []interface{}) (query string) {
	query = s.query
	return
}
