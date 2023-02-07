package mysql

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"strings"
)

func JsonObjectEquals(column string, jsonPath string, object interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" =", strings.TrimSpace(jsonPath)), object)
}

func JsonObjectGT(column string, jsonPath string, object interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" >", strings.TrimSpace(jsonPath)), object)
}

func JsonObjectGTE(column string, jsonPath string, object interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" >=", strings.TrimSpace(jsonPath)), object)
}

func JsonObjectLT(column string, jsonPath string, object interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" <", strings.TrimSpace(jsonPath)), object)
}

func JsonObjectLTE(column string, jsonPath string, object interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" <=", strings.TrimSpace(jsonPath)), object)
}

func JsonObjectBetween(column string, jsonPath string, left interface{}, right interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" BETWEEN", strings.TrimSpace(jsonPath)), left, right)
}

func JsonObjectIn(column string, jsonPath string, arguments ...interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" IN", strings.TrimSpace(jsonPath)), arguments)
}

func JsonObjectNotIn(column string, jsonPath string, arguments ...interface{}) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" NOT IN", strings.TrimSpace(jsonPath)), arguments)
}

func JsonObjectLike(column string, jsonPath string, value string) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" LIKE", strings.TrimSpace(jsonPath)), "'%"+value+"%'")
}

func JsonObjectLeftLike(column string, jsonPath string, value string) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" LIKE", strings.TrimSpace(jsonPath)), "'"+value+"%'")
}

func JsonObjectRightLike(column string, jsonPath string, value string) *dal.Condition {
	return dal.NewCondition(column, fmt.Sprintf("->> \"%s\" LIKE", strings.TrimSpace(jsonPath)), "'%"+value+"'")
}
