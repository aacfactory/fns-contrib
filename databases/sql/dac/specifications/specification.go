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
	Key       string
	Schema    string
	Name      string
	View      bool
	Type      reflect.Type
	Columns   []*Column
	Conflicts []string
	tree      []string
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

func (spec *Specification) ColumnByFieldIdx(fieldIdx int) (column *Column, has bool) {
	for _, c := range spec.Columns {
		if c.FieldIdx == fieldIdx {
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

var (
	values = sync.Map{}
	dict   = NewDict()
	group  = singleflight.Group{}
)

func GetSpecification(ctx context.Context, e any) (spec *Specification, err error) {
	table, tableErr := AsTable(e)
	if tableErr != nil {
		err = tableErr
		return
	}

	rt := reflect.TypeOf(table)

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
		s, scanErr := ScanTable(ctx, table)
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
	rv := reflect.Indirect(reflect.ValueOf(table))
	rt := rv.Type()

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

	spec = &Specification{
		Key:       key,
		Schema:    schema,
		Name:      name,
		View:      view,
		Type:      rt,
		Columns:   columns,
		Conflicts: conflicts,
		tree:      tree,
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
			if field.Type.Kind() == reflect.Ptr {
				err = errors.Warning("type of anonymous field can not be ptr").WithMeta("field", field.Name)
				return
			}
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
