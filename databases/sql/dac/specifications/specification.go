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
	Schema    string
	Name      string
	Type      reflect.Type
	Columns   []*Column
	Conflicts []string
	tree      []string
}

var (
	values = sync.Map{}
	dict   = NewDict()
	group  = singleflight.Group{}
)

func GetSpecification(ctx context.Context, table any) (spec *Specification, err error) {
	if table == nil {
		err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("table is nil"))
		return
	}
	scanned, has := values.Load(table)
	if has {
		spec, has = scanned.(*Specification)
		if !has {
			err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("stored table specification is invalid type"))
			return
		}
		return
	}

	t, isTable := table.(Table)
	if !isTable {
		err = errors.Warning("sql: get table specification failed").WithCause(fmt.Errorf("table does not implement Table"))
		return
	}

	rt := reflect.TypeOf(t)
	key := fmt.Sprintf("@fns:sql:dac:scan:%s.%s", rt.PkgPath(), rt.Name())

	processing := ctx.Value(key)
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
		ctx = context.WithValue(ctx, key, current)
		s, scanErr := ScanTable(ctx, t)
		if scanErr != nil {
			err = scanErr
			return
		}
		reflect.ValueOf(current).Elem().Set(reflect.ValueOf(s).Elem())
		v = current
		return
	})
	if err != nil {
		return
	}

	spec = scanned.(*Specification)
	return
}

func ScanTable(ctx context.Context, table Table) (spec *Specification, err error) {
	info := table.TableInfo()
	name := info.name
	if name == "" {
		err = errors.Warning("sql: scan table failed").
			WithCause(fmt.Errorf("table name is required")).
			WithMeta("struct", reflect.TypeOf(table).String())
		return
	}
	schema := info.schema
	conflicts := info.conflicts
	tree := info.tree

	rt := reflect.TypeOf(table)
	columns, columnsErr := scanTableFields(ctx, rt)
	if columnsErr != nil {
		err = errors.Warning("sql: scan table failed").
			WithCause(columnsErr).
			WithMeta("struct", reflect.TypeOf(table).String())
		return
	}

	spec = &Specification{
		Schema:    schema,
		Name:      name,
		Type:      rt,
		Columns:   columns,
		Conflicts: conflicts,
		tree:      tree,
	}

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
		if field.Anonymous {
			anonymous, anonymousErr := scanTableFields(ctx, field.Type)
			if anonymousErr != nil {

			}
		}
		if field.IsExported() {
			continue
		}
	}
	return
}
