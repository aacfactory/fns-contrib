package specifications

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"golang.org/x/sync/singleflight"
	"reflect"
	"sync"
)

type Specification struct {
	Key               string
	Schema            string
	Name              string
	View              bool
	Type              reflect.Type
	Columns           []*Column
	Conflicts         []string
	tree              []string
	queryInterceptor  bool
	queryHook         bool
	insertInterceptor bool
	insertHook        bool
	updateInterceptor bool
	updateHook        bool
	deleteInterceptor bool
	deleteHook        bool
}

func (spec *Specification) ColumnByField(fieldName string) (column *Column, has bool) {
	for _, c := range spec.Columns {
		if c.Field == fieldName {
			column = c
			has = true
			break
		}
	}
	return
}

func (spec *Specification) Tree() (parentField string, childrenField string, has bool) {
	has = len(spec.tree) == 2
	if has {
		parentField = spec.tree[0]
		childrenField = spec.tree[1]
	}
	return
}

func (spec *Specification) Pk() (v *Column, has bool) {
	for _, column := range spec.Columns {
		if column.Kind == Pk {
			v = column
			break
		}
	}
	has = v != nil
	return
}

func (spec *Specification) AuditCreation() (by *Column, at *Column, has bool) {
	n := 0
	for _, column := range spec.Columns {
		if column.Kind == Acb {
			by = column
			n++
			continue
		}
		if column.Kind == Act {
			at = column
			n++
			continue
		}
		if n == 2 {
			break
		}
	}
	has = n > 0
	return
}

func (spec *Specification) AuditModification() (by *Column, at *Column, has bool) {
	n := 0
	for _, column := range spec.Columns {
		if column.Kind == Amb {
			by = column
			n++
			continue
		}
		if column.Kind == Amt {
			at = column
			n++
			continue
		}
		if n == 2 {
			break
		}
	}
	has = n > 0
	return
}

func (spec *Specification) AuditDelete() (by *Column, at *Column, has bool) {
	n := 0
	for _, column := range spec.Columns {
		if column.Kind == Adb {
			by = column
			n++
			continue
		}
		if column.Kind == Adt {
			at = column
			n++
			continue
		}
		if n == 2 {
			break
		}
	}
	has = n > 0
	return
}

func (spec *Specification) AuditVersion() (v *Column, has bool) {
	for _, column := range spec.Columns {
		if column.Kind == Aol {
			v = column
			break
		}
	}
	has = v != nil
	return
}

func (spec *Specification) FieldScanInterfaces(instance Table, columns []int) (fields []any, err error) {
	rv := reflect.ValueOf(instance)
	for _, column := range columns {
		fieldIdx := spec.Columns[column].FieldIdx
		fv := rv.Field(fieldIdx)
		switch fv.Type().Kind() {
		case reflect.Ptr:
			nfv := reflect.New(fv.Type().Elem())
			fv.Set(nfv)
			fields = append(fields, fv.Interface())
			break
		case reflect.Slice:
			nfv := reflect.MakeSlice(fv.Type().Elem(), 0, 1)
			fv.Set(nfv)
			fields = append(fields, fv.Interface())
			break
		case reflect.Map:
			nfv := reflect.MakeMap(fv.Type())
			fv.Set(nfv)
			fields = append(fields, fv.Interface())
			break
		default:
			fields = append(fields, fv.Interface())
			break
		}
	}
	return
}

func (spec *Specification) ValueScanInterfaces(columns []int) (v Table, fields []any, err error) {
	v = reflect.New(spec.Type).Elem().Interface().(Table)
	fields, err = spec.FieldScanInterfaces(v, columns)
	return
}

var (
	values = sync.Map{}
	dict   = NewDict()
	group  = singleflight.Group{}
)

func GetSpecification(ctx context.Context, table any) (spec *Specification, err error) {
	t, isTable := table.(Table)
	if !isTable {
		err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("table does not implement Table"))
		return
	}

	rt := reflect.TypeOf(t)
	if rt.Kind() != reflect.Struct {
		err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("table does not struct"))
		return
	}

	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())

	scanned, has := values.Load(key)
	if has {
		spec, has = scanned.(*Specification)
		if !has {
			err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("stored table specification is invalid type"))
			return
		}
		return
	}

	ctxKey := fmt.Sprintf("@fns:sql:dac:scan:%s", key)

	processing := ctx.Value(ctxKey)
	if processing != nil {
		spec, has = processing.(*Specification)
		if !has {
			err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("processing table specification is invalid type"))
			return
		}
		return
	}

	scanned, err, _ = group.Do(key, func() (v interface{}, err error) {
		current := &Specification{}
		ctx = context.WithValue(ctx, ctxKey, current)
		s, scanErr := ScanTable(ctx, t)
		if scanErr != nil {
			err = scanErr
			return
		}
		reflect.ValueOf(current).Elem().Set(reflect.ValueOf(s).Elem())
		v = current
		values.Store(key, v)
		return
	})
	if err != nil {
		err = errors.Warning("sql: get table specification failed").WithCause(err)
		return
	}

	spec = scanned.(*Specification)
	return
}

func ScanTable(ctx context.Context, table Table) (spec *Specification, err error) {
	rt := reflect.TypeOf(table)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	info := table.TableInfo()
	name := info.name
	if name == "" {
		err = errors.Warning("sql: scan table failed").
			WithCause(fmt.Errorf("table name is required")).
			WithMeta("struct", rt.String())
		return
	}
	schema := info.schema
	view := info.view
	conflicts := info.conflicts
	tree := info.tree

	columns, columnsErr := scanTableFields(ctx, rt)
	if columnsErr != nil {
		err = errors.Warning("sql: scan table failed").
			WithCause(columnsErr).
			WithMeta("struct", reflect.TypeOf(table).String())
		return
	}

	ptr := reflect.New(rt)
	spec = &Specification{
		Key:               key,
		Schema:            schema,
		Name:              name,
		View:              view,
		Type:              rt,
		Columns:           columns,
		Conflicts:         conflicts,
		tree:              tree,
		queryInterceptor:  ptr.Type().Implements(queryInterceptorType),
		queryHook:         ptr.Type().Implements(queryHookType),
		insertInterceptor: ptr.Type().Implements(insertInterceptorType),
		insertHook:        ptr.Type().Implements(insertHookType),
		updateInterceptor: ptr.Type().Implements(updateInterceptorType),
		updateHook:        ptr.Type().Implements(updateHookType),
		deleteInterceptor: ptr.Type().Implements(deleteInterceptorType),
		deleteHook:        ptr.Type().Implements(deleteHookType),
	}

	tableNames := make([][]byte, 0, 1)
	if schema != "" {
		tableNames = append(tableNames, []byte(schema))
	}
	tableNames = append(tableNames, []byte(name))
	dict.Set(fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name()), tableNames...)

	return
}

func scanTableFields(ctx context.Context, rt reflect.Type) (columns []*Column, err error) {
	fields := rt.NumField()
	if fields == 0 {
		err = errors.Warning("has not field")
		return
	}
	for i := 0; i < fields; i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		if field.Anonymous {
			anonymous, anonymousErr := scanTableFields(ctx, field.Type)
			if anonymousErr != nil {
				if err != nil {
					err = errors.Warning("sql: scan table field failed").
						WithCause(anonymousErr).
						WithMeta("field", field.Name)
					return
				}
			}
			columns = append(columns, anonymous...)
			continue
		}
		column, columnErr := newColumn(ctx, i, field)
		if columnErr != nil {
			err = errors.Warning("sql: scan table field failed").
				WithCause(columnErr).
				WithMeta("field", field.Name)
			return
		}
		if column != nil {
			columns = append(columns, column)
			dict.Set(fmt.Sprintf("%s.%s:%s", rt.PkgPath(), rt.Name(), column.Field), []byte(column.Name))
		}
	}

	return
}
