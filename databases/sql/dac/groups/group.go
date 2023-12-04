package groups

import "github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"

type GroupBy struct {
	Selects []string
	Bys     []string
	Havings conditions.Condition
}

func (fields GroupBy) By(field ...string) GroupBy {
	fields.Bys = append(fields.Bys, field...)
	return fields
}

func (fields GroupBy) Having(condition conditions.Condition) GroupBy {
	fields.Havings = condition
	return fields
}

func Group(field ...string) GroupBy {
	return GroupBy{
		Selects: field,
		Bys:     nil,
	}
}
