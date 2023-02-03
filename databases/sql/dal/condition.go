package dal

import (
	"context"
	"strings"
)

func NewSubQueryArgument(model Model, column string, conditions *Conditions) *SubQueryArgument {
	return &SubQueryArgument{
		model:      model,
		column:     strings.TrimSpace(column),
		conditions: conditions,
	}
}

type SubQueryArgument struct {
	model      Model
	column     string
	conditions *Conditions
}

func (sub *SubQueryArgument) GenerateQueryFragment(ctx context.Context, dialect Dialect) (fragment string, arguments []interface{}, err error) {
	structure, getStructureErr := getModelStructure(sub.model)
	if getStructureErr != nil {
		err = getStructureErr
		return
	}
	generator, getGeneratorErr := structure.DialectQueryGenerator(dialect)
	if getGeneratorErr != nil {
		err = getGeneratorErr
		return
	}
	_, fragment, arguments, err = generator.Query(DefineSelectColumns(ctx, sub.column), sub.conditions, nil, nil)
	return
}

func NewCondition(column string, operation string, arguments ...interface{}) *Condition {
	return &Condition{
		column:    strings.TrimSpace(column),
		operator:  strings.TrimSpace(operation),
		arguments: arguments,
	}
}

type Condition struct {
	column    string
	operator  string
	arguments []interface{}
}

func (condition *Condition) Column() (column string) {
	column = condition.column
	return
}

func (condition *Condition) Operator() (operator string) {
	operator = condition.operator
	return
}

func (condition *Condition) Arguments() (arguments []interface{}) {
	arguments = condition.arguments
	return
}

type conditionFragment struct {
	operator string
	value    interface{}
}

func NewConditions(cond *Condition) *Conditions {
	fragments := make([]*conditionFragment, 0, 1)
	fragments = append(fragments, &conditionFragment{
		operator: "",
		value:    cond,
	})
	return &Conditions{
		fragments: fragments,
	}
}

type Conditions struct {
	fragments []*conditionFragment
}

func (c *Conditions) And(v *Condition) *Conditions {
	c.fragments = append(c.fragments, &conditionFragment{
		operator: "AND",
		value:    v,
	})
	return c
}

func (c *Conditions) Or(v *Condition) *Conditions {
	c.fragments = append(c.fragments, &conditionFragment{
		operator: "OR",
		value:    v,
	})
	return c
}

func (c *Conditions) AndConditions(v *Conditions) *Conditions {
	c.fragments = append(c.fragments, &conditionFragment{
		operator: "AND",
		value:    v,
	})
	return c
}

func (c *Conditions) OrConditions(v *Conditions) *Conditions {
	c.fragments = append(c.fragments, &conditionFragment{
		operator: "OR",
		value:    v,
	})
	return c
}

func (c *Conditions) Unfold(head func(condition *Condition), nextCondition func(operator string, condition *Condition), nextConditions func(operator string, conditions *Conditions)) {
	if c.fragments == nil || len(c.fragments) == 0 {
		return
	}
	if head == nil || nextCondition == nil || nextConditions == nil {
		return
	}
	head(c.fragments[0].value.(*Condition))
	for i := 1; i < len(c.fragments); i++ {
		fragment := c.fragments[i]
		operator := fragment.operator
		value := fragment.value
		switch value.(type) {
		case *Condition:
			nextCondition(operator, value.(*Condition))
			break
		case *Conditions:
			nextConditions(operator, value.(*Conditions))
		default:
			panic("fns: unknown type in conditions")
		}
	}
	return
}
