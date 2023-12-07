package conditions

import "fmt"

func Eq(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   Equal,
		Expression: expression,
	}
}

func NotEq(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   NotEqual,
		Expression: expression,
	}
}

func Gt(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   GreatThan,
		Expression: expression,
	}
}

func Gte(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   GreatThanOrEqual,
		Expression: expression,
	}
}

func Lt(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LessThan,
		Expression: expression,
	}
}

func Lte(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LessThanOrEqual,
		Expression: expression,
	}
}

func Between(field string, left any, right any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   BETWEEN,
		Expression: []any{left, right},
	}
}

func In(field string, expression ...any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   IN,
		Expression: expression,
	}
}

func NotIn(field string, expression ...any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   NOTIN,
		Expression: expression,
	}
}

func Like(field string, expression string) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LIKE,
		Expression: fmt.Sprintf("%s%%", expression),
	}
}

func LikeLast(field string, expression string) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LIKE,
		Expression: fmt.Sprintf("%%%s", expression),
	}
}

func LikeContains(field string, expression string) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LIKE,
		Expression: fmt.Sprintf("%%%s%%", expression),
	}
}

type Predicate struct {
	Field      string
	Operator   Operator
	Expression any
}

func (predicate Predicate) name() string {
	return "predicate"
}
