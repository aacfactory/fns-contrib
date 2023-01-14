package postgres

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/json"
	"strconv"
	"time"
)

func ContainsJsonObject(column string, object string) *dal.Condition {
	return dal.NewCondition(column, "@>", dal.NewLitArgument("'"+object+"'"))
}

func ContainsJsonKey(column string, key string) *dal.Condition {
	return dal.NewCondition(column, "?", dal.NewLitArgument("'"+key+"'"))
}

func ContainsJsonObjectOfArray(column string, object string) *dal.Condition {
	return dal.NewCondition(column, "?", dal.NewLitArgument("'"+object+"'"))
}

func ContainsJsonObjectsOfArray(column string, all bool, elements ...interface{}) *dal.Condition {
	values := make([]interface{}, 0, 1)
	for _, element := range elements {
		if element == nil {
			values = append(values, dal.NewLitArgument("null"))
			continue
		}
		switch element.(type) {
		case string:
			ele := element.(string)
			values = append(values, dal.NewLitArgument("'"+ele+"'"))
			break
		case int:
			ele := element.(int)
			values = append(values, dal.NewLitArgument(strconv.FormatInt(int64(ele), 10)))
			break
		case int16:
			ele := element.(int16)
			values = append(values, dal.NewLitArgument(strconv.FormatInt(int64(ele), 10)))
			break
		case int32:
			ele := element.(int32)
			values = append(values, dal.NewLitArgument(strconv.FormatInt(int64(ele), 10)))
			break
		case int64:
			ele := element.(int64)
			values = append(values, dal.NewLitArgument(strconv.FormatInt(ele, 10)))
			break
		case float32:
			ele := element.(float32)
			values = append(values, dal.NewLitArgument(fmt.Sprintf("%v", ele)))
			break
		case float64:
			ele := element.(float64)
			values = append(values, dal.NewLitArgument(fmt.Sprintf("%v", ele)))
			break
		case bool:
			ele := element.(bool)
			values = append(values, dal.NewLitArgument(fmt.Sprintf("%v", ele)))
			break
		case time.Time:
			ele := element.(time.Time)
			values = append(values, dal.NewLitArgument("'"+ele.Format(time.RFC3339)+"'"))
			break
		case json.Date:
			ele := element.(json.Date)
			values = append(values, dal.NewLitArgument("'"+ele.String()+"'"))
			break
		case sql.Date:
			ele := element.(sql.Date)
			values = append(values, dal.NewLitArgument("'"+ele.String()+"'"))
			break
		case sql.Time:
			ele := element.(sql.Time)
			values = append(values, dal.NewLitArgument("'"+ele.String()+"'"))
			break
		}
	}
	op := "?|"
	if all {
		op = "?&"
	}
	return dal.NewCondition(column, op, values...)
}
