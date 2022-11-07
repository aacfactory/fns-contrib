package postgres

import (
	"fmt"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func LitValue(v string) *Lit {
	return &Lit{value: v}
}

type Lit struct {
	value string
}

func NewSubQuery(row Table, column string, conditions *Conditions) (sub *SubQuery) {
	return &SubQuery{
		Row:        row,
		Column:     column,
		Conditions: conditions,
	}
}

type SubQuery struct {
	Row        Table
	Column     string
	Conditions *Conditions
}

func (sub *SubQuery) queryAndArguments(latestArgNum int) (query string, args []interface{}) {
	sub.Conditions.latestArgNum = latestArgNum
	tab := createOrLoadTable(sub.Row)
	query, args = tab.generateSubQuerySQL(sub.Column, sub.Conditions)
	return
}

type Condition struct {
	Column    string
	Operation string
	Values    []interface{}
}

func (c *Condition) queryAndArguments(latestArgNum int) (string, []interface{}) {
	query := ""
	args := make([]interface{}, 0, 1)
	switch c.Operation {
	case "=", "<>", ">", ">=", "<", "<=":
		query = `"` + c.Column + `" ` + c.Operation + " "
		litValue, litOk := c.Values[0].(*Lit)
		if litOk {
			query = query + litValue.value
		} else {
			latestArgNum++
			query = query + fmt.Sprintf("$%d", latestArgNum)
			args = append(args, c.Values[0])
		}
		break
	case "BETWEEN":
		query = `"` + c.Column + `" ` + c.Operation + " "
		left := c.Values[0]
		leftLit, leftLitOk := left.(*Lit)
		if leftLitOk {
			query = query + leftLit.value
		} else {
			latestArgNum++
			query = query + fmt.Sprintf("$%d", latestArgNum)
			args = append(args, left)
		}
		query = query + " AND "
		right := c.Values[1]
		rightLit, rightLitOk := right.(*Lit)
		if rightLitOk {
			query = query + rightLit.value
		} else {
			latestArgNum++
			query = query + fmt.Sprintf("$%d", latestArgNum)
			args = append(args, right)
		}
		break
	case "LIKE":
		query = `"` + c.Column + `" ` + c.Operation + " " + c.Values[0].(*Lit).value
		break
	case "IN", "NOT IN":
		query = `"` + c.Column + `" ` + c.Operation + " "
		switch c.Values[0].(type) {
		case *Lit:
			litValue := c.Values[0].(*Lit)
			sub := strings.TrimSpace(litValue.value)
			if sub[0] != '(' {
				sub = "(" + sub + ")"
			}
			query = query + sub
			break
		case *SubQuery:
			sub := c.Values[0].(*SubQuery)
			subQuery, subArguments := sub.queryAndArguments(latestArgNum)
			query = query + "(" + subQuery + ")"
			if subArguments != nil {
				latestArgNum = latestArgNum + len(subArguments)
				args = append(args, subArguments...)
			}
			break
		default:
			sub := ""
			for _, value := range c.Values {
				latestArgNum++
				sub = sub + "," + fmt.Sprintf("$%d", latestArgNum)
				args = append(args, value)
			}
			if len(sub) > 0 {
				sub = sub[1:]
			}
			query = query + "(" + sub + ")"
			break
		}
		break
	case "@>":
		litValue := c.Values[0].(*Lit)
		query = `"` + c.Column + `" ` + c.Operation + " " + litValue.value
		break
	case "?":
		litValue := c.Values[0].(*Lit)
		query = `"` + c.Column + `" ` + c.Operation + " " + litValue.value
		break
	case "?&", "?|":
		vv := make([]string, 0, 1)
		for _, value := range c.Values {
			litValue := value.(*Lit)
			vv = append(vv, litValue.value)
		}
		query = `"` + c.Column + `" ` + c.Operation + " array[" + strings.Join(vv, ",") + "]"
		break
	}
	return query, args
}

func LitCond(column string, operator string, value string) *Condition {
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: operator,
		Values:    []interface{}{LitValue(value)},
	}
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

func BetweenTime(column string, tr *times.TimeRange) *Condition {
	if tr == nil {
		tr = &times.TimeRange{}
	}
	if tr.IsZero() {
		tr.End = time.Now().AddDate(0, 0, 1)
	}
	if tr.End.IsZero() {
		tr.End = time.Now().AddDate(0, 0, 1)
	}
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "BETWEEN",
		Values:    []interface{}{tr.Beg, tr.End},
	}
}

func BetweenDate(column string, dr *times.DateRange) *Condition {
	if dr == nil {
		dr = &times.DateRange{}
	}
	if dr.IsZero() {
		dr.End = json.NewDateFromTime(time.Now().AddDate(0, 0, 1))
	}
	if dr.End.IsZero() {
		dr.End = json.NewDateFromTime(time.Now().AddDate(0, 0, 1))
	}
	return &Condition{
		Column:    strings.TrimSpace(column),
		Operation: "BETWEEN",
		Values:    []interface{}{dr.Beg, dr.End},
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
	if litValue, litOk := value.(*Lit); litOk {
		return &Condition{
			Column:    strings.TrimSpace(column),
			Operation: "IN",
			Values:    []interface{}{litValue},
		}
	}
	if sub, subOk := value.(*SubQuery); subOk {
		return &Condition{
			Column:    strings.TrimSpace(column),
			Operation: "IN",
			Values:    []interface{}{sub},
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
	if litValue, litOk := value.(*Lit); litOk {
		return &Condition{
			Column:    strings.TrimSpace(column),
			Operation: "NOT IN",
			Values:    []interface{}{litValue},
		}
	}
	if sub, subOk := value.(*SubQuery); subOk {
		return &Condition{
			Column:    strings.TrimSpace(column),
			Operation: "NOT IN",
			Values:    []interface{}{sub},
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

func ContainsJsonObject(column string, object string) *Condition {
	return &Condition{
		Column:    column,
		Operation: "@>",
		Values:    []interface{}{LitValue("'" + object + "'")},
	}
}

func ContainsJsonKey(column string, key string) *Condition {
	return &Condition{
		Column:    column,
		Operation: "?",
		Values:    []interface{}{LitValue("'" + key + "'")},
	}
}

func ContainsJsonObjectOfArray(column string, object string) *Condition {
	return &Condition{
		Column:    column,
		Operation: "?",
		Values:    []interface{}{LitValue("'" + object + "'")},
	}
}

func ContainsJsonObjectsOfArray(column string, all bool, elements ...interface{}) *Condition {
	values := make([]interface{}, 0, 1)
	for _, element := range elements {
		if element == nil {
			values = append(values, LitValue("null"))
			continue
		}
		switch element.(type) {
		case string:
			ele := element.(string)
			values = append(values, LitValue("'"+ele+"'"))
			break
		case int:
			ele := element.(int)
			values = append(values, LitValue(strconv.FormatInt(int64(ele), 10)))
			break
		case int16:
			ele := element.(int16)
			values = append(values, LitValue(strconv.FormatInt(int64(ele), 10)))
			break
		case int32:
			ele := element.(int32)
			values = append(values, LitValue(strconv.FormatInt(int64(ele), 10)))
			break
		case int64:
			ele := element.(int64)
			values = append(values, LitValue(strconv.FormatInt(ele, 10)))
			break
		case float32:
			ele := element.(float32)
			values = append(values, LitValue(fmt.Sprintf("%v", ele)))
			break
		case float64:
			ele := element.(float64)
			values = append(values, LitValue(fmt.Sprintf("%v", ele)))
			break
		case bool:
			ele := element.(bool)
			values = append(values, LitValue(fmt.Sprintf("%v", ele)))
			break
		case time.Time:
			ele := element.(time.Time)
			values = append(values, LitValue("'"+ele.Format(time.RFC3339)+"'"))
			break
		case json.Date:
			ele := element.(json.Date)
			values = append(values, LitValue("'"+ele.String()+"'"))
			break
		}
	}
	op := "?|"
	if all {
		op = "?&"
	}
	return &Condition{
		Column:    column,
		Operation: op,
		Values:    values,
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
	latestArgNum int
	units        []*conditionUnit
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
			sub, subArgs := v.queryAndArguments(len(args) + c.latestArgNum)
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
