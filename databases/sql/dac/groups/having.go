package groups

import "github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"

type HavingCondition struct {
	Cond conditions.Condition
}

func Having(cond conditions.Condition) HavingCondition {
	return HavingCondition{
		Cond: cond,
	}
}
