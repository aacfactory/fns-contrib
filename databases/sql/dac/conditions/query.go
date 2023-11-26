package conditions

func Query(query any, field string, cond Condition) QueryExpr {
	return QueryExpr{
		Query: query,
		Field: field,
		Cond:  cond,
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
	Query any
	Field string
	Cond  Condition
}
