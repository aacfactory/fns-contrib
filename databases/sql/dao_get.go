package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"reflect"
)

func (d *dao) Get(ctx fns.Context, row TableRow) (has bool, err errors.CodeError) {
	if row == nil {
		panic(fmt.Sprintf("fns SQL: use DAO failed for row is nil"))
	}
	hasGot, synced := d.Cache.GetAndFill(row)
	if hasGot {
		has = true
	}
	if synced {
		return
	}
	info := getTableRowInfo(row)
	rv := reflect.Indirect(reflect.ValueOf(row))
	if !hasGot {
		query := info.GetQuery.Query
		paramFields := info.GetQuery.Params
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
			d.Cache.Set(row, true)
			return
		}
		scanErr := rows.Scan(row)
		if scanErr != nil {
			err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Get").WithCause(scanErr)
			return
		}
		d.Cache.Set(row, false)
		has = true
	}

	// fk
	for _, column := range info.ForeignColumns {
		frv := rv.FieldByName(column.StructFieldName)
		if frv.IsNil() {
			continue
		}
		x := frv.Interface()
		xRow, mapOk := x.(TableRow)
		if !mapOk {
			panic(fmt.Sprintf("fns SQL: use DAO failed for %s of row is not sql.TableRow", column.StructFieldName))
		}
		_, loadFCErr := d.Get(ctx, xRow)
		if loadFCErr != nil {
			err = loadFCErr
			return
		}
		frv.Set(reflect.ValueOf(x))
	}
	// vc
	if info.VirtualQuery != nil {
		query := info.VirtualQuery.Query
		paramFields := info.VirtualQuery.Params
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
		if queryErr != nil {
			err = queryErr
			return
		}
		if rows.Empty() {
			d.Cache.Set(row, true)
			return
		}
		scanErr := rows.Scan(row)
		if scanErr != nil {
			err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Get").WithCause(scanErr)
			return
		}
	}
	// lk
	for _, column := range info.LinkColumns {
		lrv := rv.FieldByName(column.StructFieldName)
		lkHasRef := false
		if lrv.Len() == 0 {
			lkInfo := getTableRowInfo(reflect.New(column.ElementType.Elem()).Interface())
			lkHasRef = len(lkInfo.ForeignColumns) != 0 || len(lkInfo.LinkColumns) != 0 || lkInfo.VirtualQuery != nil
			linkQuery := lkInfo.genLinkQuery(column)
			leftField := info.GetColumnField(column.LeftColumn)
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
			scanLinkErr := linkRows.Scan(&x)
			if scanLinkErr != nil {
				err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Get").WithCause(scanLinkErr)
				return
			}

			lrv.Set(reflect.ValueOf(x))
		}
		if lkHasRef {
			size := lrv.Len()
			for i := 0; i < size; i++ {
				rvxe := lrv.Index(i).Interface()
				rvxr := rvxe.(TableRow)
				d.Cache.Set(rvxr, false)
				_, loadLCErr := d.Get(ctx, rvxr)
				if loadLCErr != nil {
					err = loadLCErr
					return
				}
			}
		}
	}
	d.Cache.Set(row, true)
	return
}
