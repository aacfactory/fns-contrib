package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"reflect"
)

func (d *dao) Get(ctx fns.Context) (has bool, err errors.CodeError) {
	if d.TargetIsArray {
		has, err = d.getArray(ctx)
	} else {
		has, err = d.getOne(ctx)
	}
	return
}

func (d *dao) getArray(ctx fns.Context) (has bool, err errors.CodeError) {
	rv := reflect.Indirect(reflect.ValueOf(d.Target))
	size := rv.Len()
	if size < 1 {
		return
	}
	loaded := 0
	for i := 0; i < size; i++ {
		v := rv.Index(i)
		if v.IsNil() {
			continue
		}
		x := v.Interface()
		xDAO := newDAO(x, d.Loaded, d.Affected)
		xHas, xErr := xDAO.getOne(ctx)
		if xErr != nil {
			err = xErr
			return
		}
		if xHas {
			loaded++
		}
	}
	has = loaded == size
	return
}

func (d *dao) getOne(ctx fns.Context) (has bool, err errors.CodeError) {
	rv := reflect.Indirect(reflect.ValueOf(d.Target))
	pks := make([]interface{}, 0, 1)
	for _, pk := range d.TableInfo.Pks {
		pks = append(pks, rv.FieldByName(pk.StructFieldName).Interface())
	}
	loaded, hasLoaded := d.getLoaded(pks)
	if hasLoaded {
		has = true
		reflect.ValueOf(d.Target).Set(reflect.ValueOf(loaded))
		return
	}

	query := d.TableInfo.GetQuery.Query
	paramFields := d.TableInfo.GetQuery.Params

	params := NewTuple()
	for _, field := range paramFields {
		params.Append(rv.FieldByName(field).Interface())
	}

	rows, queryErr := Query(ctx, Param{
		Query: query,
		Args:  params,
	})
	if queryErr != nil {
		err = queryErr
		return
	}

	if rows.Empty() {
		return
	}

	scanErr := rows.Scan(d.Target)
	if scanErr != nil {
		err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Get").WithCause(scanErr)
		return
	}

	// fk
	for _, column := range d.TableInfo.ForeignColumns {
		frv := rv.FieldByName(column.StructFieldName)
		if frv.IsNil() {
			continue
		}
		fd := newDAO(frv.Interface(), d.Loaded, d.Affected)
		_, loadFCErr := fd.getOne(ctx)
		if loadFCErr != nil {
			err = loadFCErr
			return
		}
	}
	// lk
	for _, column := range d.TableInfo.LinkColumns {
		lrv := rv.FieldByName(column.StructFieldName)
		lkInfo := newTableInfo(reflect.New(column.ElementType.Elem()).Interface(), d.Driver)
		linkQuery := lkInfo.genLinkQuery(column)
		leftField := d.TableInfo.GetColumnField(column.LeftColumn)
		left := rv.FieldByName(leftField).Interface()
		lkParams := NewTuple().Append(left)
		linkRows, queryLinkErr := Query(ctx, Param{
			Query: linkQuery,
			Args:  lkParams,
		})
		if queryLinkErr != nil {
			err = queryLinkErr
			return
		}
		if linkRows.Empty() {
			continue
		}
		x := reflect.MakeSlice(column.SliceType, 0, 1).Interface()
		scanLinkErr := rows.Scan(&x)
		if scanLinkErr != nil {
			err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Get").WithCause(scanLinkErr)
			return
		}
		lrv.Set(reflect.ValueOf(x))
	}

	return
}
