package postgres

import "github.com/aacfactory/fns-contrib/databases/sql"

type Select interface {
	Build(args *sql.Tuple) (query string)
}

func LitSelect(query string) Select {
	return &litSelect{
		query: query,
	}
}

type litSelect struct {
	query string
}

func (s *litSelect) Build(_ *sql.Tuple) (query string) {
	query = s.query
	return
}
