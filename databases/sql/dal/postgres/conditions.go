package postgres

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/json"
	"strconv"
	"time"
)

func ContainsJsonObject(column string, object string) *dal.Condition {
	return dal.NewCondition(column, "@>", "'"+object+"'")
}

func ContainsJsonKey(column string, key string) *dal.Condition {
	return dal.NewCondition(column, "?", "'"+key+"'")
}

func ContainsJsonObjectOfArray(column string, object string) *dal.Condition {
	return dal.NewCondition(column, "?", "'"+object+"'")
}

func ContainsJsonObjectsOfArray(column string, all bool, elements ...interface{}) *dal.Condition {
	values := make([]interface{}, 0, 1)
	for _, element := range elements {
		if element == nil {
			values = append(values, "null")
			continue
		}
		switch element.(type) {
		case string:
			ele := element.(string)
			values = append(values, "'"+ele+"'")
			break
		case int:
			ele := element.(int)
			values = append(values, strconv.FormatInt(int64(ele), 10))
			break
		case int16:
			ele := element.(int16)
			values = append(values, strconv.FormatInt(int64(ele), 10))
			break
		case int32:
			ele := element.(int32)
			values = append(values, strconv.FormatInt(int64(ele), 10))
			break
		case int64:
			ele := element.(int64)
			values = append(values, strconv.FormatInt(ele, 10))
			break
		case float32:
			ele := element.(float32)
			values = append(values, fmt.Sprintf("%v", ele))
			break
		case float64:
			ele := element.(float64)
			values = append(values, fmt.Sprintf("%v", ele))
			break
		case bool:
			ele := element.(bool)
			values = append(values, fmt.Sprintf("%v", ele))
			break
		case time.Time:
			ele := element.(time.Time)
			values = append(values, "'"+ele.Format(time.RFC3339)+"'")
			break
		case json.Date:
			ele := element.(json.Date)
			values = append(values, "'"+ele.String()+"'")
			break
		case sql.Date:
			ele := element.(sql.Date)
			values = append(values, "'"+ele.String()+"'")
			break
		case sql.Time:
			ele := element.(sql.Time)
			values = append(values, "'"+ele.String()+"'")
			break
		}
	}
	op := "?|"
	if all {
		op = "?&"
	}
	return dal.NewCondition(column, op, values...)
}

const (
	conditionsArgumentNumCtxKey = "@fns_sql_dal_pg_conditions_arg_num"
)

func setGenericConditionsArgumentNum(ctx context.Context) context.Context {
	if getGenericConditionsArgumentNum(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, conditionsArgumentNumCtxKey, &genericConditionsArgumentNum{
		value: 0,
	})
}

func getGenericConditionsArgumentNum(ctx context.Context) (v *genericConditionsArgumentNum) {
	x := ctx.Value(conditionsArgumentNumCtxKey)
	if x == nil {
		return
	}
	v = x.(*genericConditionsArgumentNum)
	return
}

type genericConditionsArgumentNum struct {
	value int
}

func (c *genericConditionsArgumentNum) Incr() (num int) {
	c.value = c.value + 1
	num = c.Value()
	return
}

func (c *genericConditionsArgumentNum) Value() (num int) {
	num = c.value
	return
}