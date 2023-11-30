package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"reflect"
)

func NewTableInfo(schema string, name string, view bool, conflicts []string, tree []string) TableInfo {
	return TableInfo{schema: schema, name: name, view: view, conflicts: conflicts, tree: tree}
}

type TableInfo struct {
	schema    string
	name      string
	view      bool
	conflicts []string
	tree      []string
}

type Table interface {
	TableInfo() TableInfo
}

func TableInstance[T Table]() (v T) {
	return
}

func AsTable(e any) (t Table, err error) {
	table, ok := e.(Table)
	if ok {
		t = table
		return
	}
	rt := reflect.TypeOf(e)
	if rt.Kind() == reflect.Ptr && rt.Elem().Kind() == reflect.Struct {
		e = reflect.Zero(rt.Elem()).Interface()
		table, ok = e.(Table)
		if ok {
			t = table
			return
		}
		err = errors.Warning(fmt.Sprintf("sql: %s.%s does not implement Table", rt.PkgPath(), rt.Name()))
		return
	}
	err = errors.Warning(fmt.Sprintf("sql: %s.%s does not implement Table", rt.PkgPath(), rt.Name()))
	return
}
