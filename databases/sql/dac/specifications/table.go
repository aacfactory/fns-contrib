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
	switch rt.Kind() {
	case reflect.Struct:
		e = reflect.New(rt).Interface()
		table, ok = e.(Table)
		if ok {
			t = table
			return
		}
		err = errors.Warning(fmt.Sprintf("sql: %s.%s does not implement Table", rt.PkgPath(), rt.Name()))
		return
	case reflect.Ptr:
		e = reflect.Zero(rt.Elem()).Interface()
		table, ok = e.(Table)
		if ok {
			t = table
			return
		}
		err = errors.Warning(fmt.Sprintf("sql: %s.%s does not implement Table", rt.PkgPath(), rt.Name()))
		return
	default:
		err = errors.Warning(fmt.Sprintf("sql: %s.%s does not implement Table", rt.PkgPath(), rt.Name()))
		return
	}
}
