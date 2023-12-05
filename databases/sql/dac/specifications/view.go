package specifications

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"reflect"
	"strings"
)

type ViewInfo struct {
	pure   bool
	name   string
	schema string
	base   any
}

func MaybeView(e any) (ok bool) {
	rv := reflect.Indirect(reflect.ValueOf(e))
	rt := rv.Type()
	_, ok = rt.MethodByName("ViewInfo")
	return
}

func GetViewInfo(e any) (info ViewInfo, err error) {
	rv := reflect.Indirect(reflect.ValueOf(e))
	rt := rv.Type()
	// info
	_, hasInfoFunc := rt.MethodByName("ViewInfo")
	if !hasInfoFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not ViewInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	infoFunc := rv.MethodByName("ViewInfo")
	results := infoFunc.Call(nil)
	if len(results) != 1 {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid ViewInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	result := results[0]
	// pure
	_, hasPureFunc := result.Type().MethodByName("Pure")
	if !hasPureFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not ViewInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	nameResults := result.MethodByName("Pure").Call(nil)
	if len(nameResults) != 3 && nameResults[0].Type().Kind() != reflect.String && nameResults[1].Type().Kind() != reflect.String && nameResults[2].Type().Kind() != reflect.Bool {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid ViewInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	schema := nameResults[0].String()
	name := nameResults[1].String()
	pure := nameResults[2].Bool()
	if pure {
		info = ViewInfo{
			pure:   pure,
			name:   strings.TrimSpace(name),
			schema: strings.TrimSpace(schema),
			base:   nil,
		}
		return
	}
	// base
	_, hasBaseFunc := result.Type().MethodByName("Base")
	if !hasBaseFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not ViewInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	baseResults := result.MethodByName("Base").Call(nil)
	if len(baseResults) != 1 && nameResults[0].Type().Kind() != reflect.Interface {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid ViewInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	base := baseResults[0].Interface()
	info = ViewInfo{
		pure:   false,
		name:   "",
		schema: "",
		base:   base,
	}
	return
}

func ScanView(ctx context.Context, view any) (spec *Specification, err error) {
	rv := reflect.Indirect(reflect.ValueOf(view))
	rt := rv.Type()
	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	info, infoErr := GetViewInfo(view)
	if infoErr != nil {
		err = errors.Warning("sql: scan view failed").
			WithCause(infoErr).
			WithMeta("struct", key)
		return
	}
	if info.pure {
		name := info.name
		if name == "" {
			err = errors.Warning("sql: scan view failed").
				WithCause(fmt.Errorf("table name is required")).
				WithMeta("struct", rt.String())
			return
		}
		schema := info.schema
		columns, columnsErr := scanTableFields(ctx, fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name()), rt)
		if columnsErr != nil {
			err = errors.Warning("sql: scan view failed").
				WithCause(columnsErr).
				WithMeta("struct", reflect.TypeOf(view).String())
			return
		}
		spec = &Specification{
			Key:     key,
			Schema:  schema,
			Name:    name,
			View:    true,
			Type:    rt,
			Columns: columns,
		}
		tableNames := make([][]byte, 0, 1)
		if schema != "" {
			tableNames = append(tableNames, []byte(schema))
		}
		tableNames = append(tableNames, []byte(name))
		dict.Set(fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name()), tableNames...)
	} else {
		base, baseErr := GetSpecification(ctx, info.base)
		if baseErr != nil {
			err = errors.Warning("sql: scan view failed").
				WithCause(baseErr).
				WithMeta("struct", reflect.TypeOf(view).String())
			return
		}
		columns, columnsErr := scanTableFields(ctx, fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name()), rt)
		if columnsErr != nil {
			err = errors.Warning("sql: scan view failed").
				WithCause(columnsErr).
				WithMeta("struct", reflect.TypeOf(view).String())
			return
		}
		spec = &Specification{
			Key:      key,
			Schema:   base.Schema,
			Name:     base.Name,
			View:     true,
			ViewBase: base,
			Type:     rt,
			Columns:  columns,
		}
		tableNames := make([][]byte, 0, 1)
		if base.Schema != "" {
			tableNames = append(tableNames, []byte(base.Schema))
		}
		tableNames = append(tableNames, []byte(base.Name))
		dict.Set(fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name()), tableNames...)
	}
	return
}
