package dal

import (
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

func Eq(column string, value interface{}) *Condition {
	return NewCondition(column, "=", value)
}

func NotEq(column string, value interface{}) *Condition {
	return NewCondition(column, "<>", value)
}

func GT(column string, value interface{}) *Condition {
	return NewCondition(column, ">", value)
}

func GTE(column string, value interface{}) *Condition {
	return NewCondition(column, ">=", value)
}

func LT(column string, value interface{}) *Condition {
	return NewCondition(column, "<", value)
}

func LTE(column string, value interface{}) *Condition {
	return NewCondition(column, "<=", value)
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
	return NewCondition(column, "BETWEEN", tr.Beg, tr.End)
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
	return NewCondition(column, "BETWEEN", dr.Beg, dr.End)
}

func Between(column string, left interface{}, right interface{}) *Condition {
	return NewCondition(column, "BETWEEN", left, right)
}

func IN(column string, value interface{}) *Condition {
	switch value.(type) {
	case *SubQueryArgument:
		return NewCondition(column, "IN", value)
	default:
		values := make([]interface{}, 0, 1)
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			values = append(values, rv.Index(i).Interface())
		}
		return NewCondition(column, "IN", values...)
	}
}

func NotIn(column string, value interface{}) *Condition {
	switch value.(type) {
	case *SubQueryArgument:
		return NewCondition(column, "NOT IN", value)
	default:
		values := make([]interface{}, 0, 1)
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			values = append(values, rv.Index(i).Interface())
		}
		return NewCondition(column, "NOT IN", values...)
	}
}

func Like(column string, value string) *Condition {
	return NewCondition(column, "LIKE", "'%"+value+"%'")
}

func LikeLeft(column string, value string) *Condition {
	return NewCondition(column, "LIKE", "'"+value+"%'")
}

func LikeRight(column string, value string) *Condition {
	return NewCondition(column, "LIKE", "'%"+value+"'")
}
