package conditions

func Aggregate(query any, fn string, field string, cond Condition) AggregateExpr {
	return AggregateExpr{
		Query: query,
		Func:  fn,
		Field: field,
		Cond:  cond,
	}
}

type AggregateExpr struct {
	Query any
	Func  string
	Field string
	Cond  Condition
}
