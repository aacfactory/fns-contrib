package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"reflect"
	"strings"
)

type TableInfo struct {
	schema    string
	name      string
	conflicts []string
}

func MaybeTable(e any) (ok bool) {
	rv := reflect.Indirect(reflect.ValueOf(e))
	rt := rv.Type()
	_, ok = rt.MethodByName("TableInfo")
	return
}

func GetTableInfo(e any) (info TableInfo, err error) {
	rv := reflect.Indirect(reflect.ValueOf(e))
	rt := rv.Type()
	// info
	_, hasInfoFunc := rt.MethodByName("TableInfo")
	if !hasInfoFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	infoFunc := rv.MethodByName("TableInfo")
	results := infoFunc.Call(nil)
	if len(results) != 1 {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	result := results[0]
	// name
	_, hasNameFunc := result.Type().MethodByName("Name")
	if !hasNameFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	nameResults := result.MethodByName("Name").Call(nil)
	if len(nameResults) != 1 && nameResults[0].Type().Kind() != reflect.String {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	name := nameResults[0].String()
	// schema
	_, hasSchemaFunc := result.Type().MethodByName("Schema")
	if !hasSchemaFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	schemaResults := result.MethodByName("Schema").Call(nil)
	if len(schemaResults) != 1 && schemaResults[0].Type().Kind() != reflect.String {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	schema := schemaResults[0].String()
	// conflicts
	_, hasConflictsFunc := result.Type().MethodByName("Conflicts")
	if !hasConflictsFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	conflictsResults := result.MethodByName("Conflicts").Call(nil)
	if len(conflictsResults) != 1 && conflictsResults[0].Type().Kind() != reflect.Slice && conflictsResults[0].Type().Elem().Kind() != reflect.String {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid TableInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	conflicts := conflictsResults[0].Interface().([]string)
	for i, conflict := range conflicts {
		conflicts[i] = strings.TrimSpace(conflict)
	}
	// view
	info = TableInfo{
		schema:    strings.TrimSpace(schema),
		name:      strings.TrimSpace(name),
		conflicts: conflicts,
	}
	return
}
