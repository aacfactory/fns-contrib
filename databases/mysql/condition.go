package mysql

import (
	"reflect"
	"strings"
)

func LitValue(v string) *lit {
	return &lit{value: v}
}

type lit struct {
	value string
}

type Condition struct {
	Column    string
	Operation string
	Values    []interface{}
}

func (c *Condition) queryAndArguments() (string, []interface{}) {
	query := ""
	args := make([]interface{}, 0, 1)
	switch c.Operation {
	case "=", "<>", ">", ">=", "<", "<=":
		query = "`" + c.Column + "` " + c.Operation + " "
		litValue, litOk := c.Values[0].(*lit)
		if litOk {
			query = query + litValue.value
		} else {
			query = query + "?"
			args = append(args, c.Values[0])
		}
	case "BETWEEN":
		query = "`" + c.Column + "` " + c.Operation + " "
		left := c.Values[0]
		leftLit, leftLitOk := left.(*lit)
		if leftLitOk {
			query = query + leftLit.value
		} else {
			query = query + "?"
			args = append(args, left)
		}
		query = query + " AND "
		right := c.Values[1]
		rightLit, rightLitOk := right.(*lit)
		if rightLitOk {
			query = query + rightLit.value
		} else {
			query = query + "?"
			args = append(args, right)
		}
	case "LIKE":
		query = "`" + c.Column + "` " + c.Operation + " " + c.Values[0].(*lit).value
	case "IN", "NOT IN":
		query = "`" + c.Column + "` " + c.Operation + " "
		litValue, litOk := c.Values[0].(*lit)
		if litOk {
			sub := strings.TrimSpace(litValue.value)
			if sub[0] != '(' {
				sub = "(" + sub + ")"
			}
			query = query + sub
		} else {
			sub := ""
			for _, value := range c.Values {
				sub = sub + "," + "?"
				args = append(args, value)
			}
			if len(sub) > 0 {
				sub = sub[1:]
			}
			query = query + "(" + sub + ")"

		}
	}
	return query, args
}

func Eq(column string, value interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "=",
		Values:    []interface{}{value},
	}
}

func NotEq(column string, value interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "<>",
		Values:    []interface{}{value},
	}
}

func GT(column string, value interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: ">",
		Values:    []interface{}{value},
	}
}

func GTE(column string, value interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: ">=",
		Values:    []interface{}{value},
	}
}

func LT(column string, value interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "<",
		Values:    []interface{}{value},
	}
}

func LTE(column string, value interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "<=",
		Values:    []interface{}{value},
	}
}

func Between(column string, left interface{}, right interface{}) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "BETWEEN",
		Values:    []interface{}{left, right},
	}
}

func IN(column string, value interface{}) *Condition {
	if litValue, litOk := value.(*lit); litOk {
		return &Condition{
			Column:    strings.TrimSpace(column),
			Operation: "IN",
			Values:    []interface{}{litValue},
		}
	}
	values := make([]interface{}, 0, 1)
	rv := reflect.ValueOf(value)
	for i := 0; i < rv.Len(); i++ {
		values = append(values, rv.Index(i).Interface())
	}
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "IN",
		Values:    values,
	}
}

func NotIn(column string, value interface{}) *Condition {
	if litValue, litOk := value.(*lit); litOk {
		return &Condition{
			Column:    strings.TrimSpace(column),
			Operation: "NOT IN",
			Values:    []interface{}{litValue},
		}
	}
	values := make([]interface{}, 0, 1)
	rv := reflect.ValueOf(value)
	for i := 0; i < rv.Len(); i++ {
		values = append(values, rv.Index(i).Interface())
	}
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "NOT IN",
		Values:    values,
	}
}

func Like(column string, value string) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "LIKE",
		Values:    []interface{}{LitValue("'%" + value + "%'")},
	}
}

func LikeLeft(column string, value string) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "LIKE",
		Values:    []interface{}{LitValue("'" + value + "%'")},
	}
}

func LikeRight(column string, value string) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "LIKE",
		Values:    []interface{}{LitValue("'%" + value + "'")},
	}
}

func NotDeleted(deletedByColumnName string) *Condition {
	return &Condition{
		Column:    deletedByColumnName,
		Operation: "=",
		Values:    []interface{}{LitValue("''")},
	}
}

type conditionUnit struct {
	andOr string
	value interface{}
}

func NewConditions(cond *Condition) *Conditions {
	units := make([]*conditionUnit, 0, 1)
	units = append(units, &conditionUnit{
		andOr: "",
		value: cond,
	})
	return &Conditions{
		units: units,
	}
}

type Conditions struct {
	units []*conditionUnit
}

func (c *Conditions) And(v *Condition) *Conditions {
	c.units = append(c.units, &conditionUnit{
		andOr: "AND",
		value: v,
	})
	return c
}

func (c *Conditions) Or(v *Condition) *Conditions {
	c.units = append(c.units, &conditionUnit{
		andOr: "OR",
		value: v,
	})
	return c
}

func (c *Conditions) AndConditions(v *Conditions) *Conditions {
	c.units = append(c.units, &conditionUnit{
		andOr: "AND",
		value: v,
	})
	return c
}

func (c *Conditions) OrConditions(v *Conditions) *Conditions {
	c.units = append(c.units, &conditionUnit{
		andOr: "OR",
		value: v,
	})
	return c
}

func (c *Conditions) QueryAndArguments() (query string, args []interface{}) {
	query, args = c.queryAndArguments()
	query = query[1 : len(query)-1]
	return
}

func (c *Conditions) queryAndArguments() (query string, args []interface{}) {
	args = make([]interface{}, 0, 1)
	for _, unit := range c.units {
		switch unit.value.(type) {
		case *Condition:
			v := unit.value.(*Condition)
			sub, subArgs := v.queryAndArguments()
			if unit.andOr != "" {
				query = query + " " + unit.andOr + " " + sub
			} else {
				query = query + sub
			}
			if len(subArgs) > 0 {
				args = append(args, subArgs...)
			}
		case *Conditions:
			v := unit.value.(*Conditions)
			sub, subArgs := v.queryAndArguments()
			if unit.andOr != "" {
				query = query + " " + unit.andOr + " " + sub
			} else {
				query = query + sub
			}
			if len(subArgs) > 0 {
				args = append(args, subArgs...)
			}
		}
	}
	query = "(" + query + ")"
	return
}
