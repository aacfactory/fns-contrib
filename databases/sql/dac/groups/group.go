package groups

import "github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"

type GroupBy struct {
	Bys     []string
	Havings conditions.Condition
}

func (fields GroupBy) Having(condition conditions.Condition) GroupBy {
	fields.Havings = condition
	return fields
}

func Group(field ...string) GroupBy {
	return GroupBy{
		Bys: field,
	}
}
