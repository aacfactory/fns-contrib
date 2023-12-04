package conditions

func Query(query any, field string, cond Condition) QueryExpr {
	return QueryExpr{
		Query: query,
		Field: field,
		Cond:  cond,
	}
}

func AggregateQuery(query any, aggregate string, field string, cond Condition) QueryExpr {
	return QueryExpr{
		Query:     query,
		Aggregate: aggregate,
		Field:     field,
		Cond:      cond,
	}
}

func LitQuery(query string) QueryExpr {
	return QueryExpr{
		Query: query,
		Field: "",
		Cond:  Condition{},
	}
}

type QueryExpr struct {
	Query     any
	Aggregate string
	Field     string
	Cond      Condition
}

const (
	AVG   = "AVG"
	SUM   = "SUM"
	COUNT = "COUNT"
	MAX   = "MAX"
	MIN   = "MIN"
)
