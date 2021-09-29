package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"reflect"
	"strconv"
	"time"
)

func (d *dao) Delete(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError) {
	if rows == nil || len(rows) == 0 {
		affected = 0
		return
	}
	txErr := d.beginTx(ctx)
	if txErr != nil {
		err = errors.ServiceError("fns SQL: dao begin tx failed").WithCause(txErr)
		return
	}

	for _, row := range rows {
		if row == nil {
			continue
		}
		affected0, err0 := d.deleteOne(ctx, row)
		if err0 != nil {
			err = err0
			return
		}
		affected = affected + affected0
	}
	cmtErr := d.commitTx(ctx)
	if cmtErr != nil {
		err = errors.ServiceError("fns SQL: dao commit tx failed").WithCause(cmtErr)
		return
	}
	return
}

func (d *dao) deleteOne(ctx fns.Context, row TableRow) (affected int, err errors.CodeError) {
	if d.hasAffected(row) {
		return
	}
	info := getTableRowInfo(row)
	query := info.DeleteQuery.Query
	paramFields := info.DeleteQuery.Params

	rt := reflect.TypeOf(row).Elem()
	rv := reflect.Indirect(reflect.ValueOf(row))

	fcs := make([]TableRow, 0, 1)
	for _, column := range info.ForeignColumns {
		if column.Sync {
			fv := rv.FieldByName(column.StructFieldName)
			if fv.Len() > 0 {
				for i := 0; i < fv.Len(); i++ {
					fcv := fv.Index(i).Interface()
					fcr, ok := fcv.(TableRow)
					if !ok {
						panic(fmt.Sprintf("fns SQL: use DAO failed for %s of row is not sql.TableRow", column.StructFieldName))
					}
					fcs = append(fcs, fcr)
				}
			}
		}
	}
	lcs := make([]TableRow, 0, 1)
	for _, column := range info.LinkColumns {
		if column.Sync {
			fv := rv.FieldByName(column.StructFieldName)
			if fv.Len() > 0 {
				for i := 0; i < fv.Len(); i++ {
					lcv := fv.Index(i).Interface()
					lcr, ok := lcv.(TableRow)
					if !ok {
						panic(fmt.Sprintf("fns SQL: use DAO failed for %s of row is not sql.TableRow", column.StructFieldName))
					}
					lcs = append(lcs, lcr)
				}
			}
		}
	}

	params := NewTuple()
	// delete by
	if info.DeleteBY != nil {
		deleteBy := ctx.User().Id()
		if deleteBy != "" {
			rct, _ := rt.FieldByName(info.DeleteBY.StructFieldName)
			rvv := rv.FieldByName(info.DeleteBY.StructFieldName)
			switch rct.Type.Kind() {
			case reflect.String:
				rvv.SetString(deleteBy)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				userId, parseErr := strconv.Atoi(deleteBy)
				if parseErr == nil {
					rvv.SetInt(int64(userId))
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				userId, parseErr := strconv.Atoi(deleteBy)
				if parseErr == nil {
					rvv.SetUint(uint64(userId))
				}
			}
		}
	}
	// delete at
	if info.DeleteAT != nil {
		rvv := rv.FieldByName(info.DeleteAT.StructFieldName)
		rvv.Set(reflect.ValueOf(time.Now()))
	}
	for _, field := range paramFields {
		fv := rv.FieldByName(field)
		// builtin
		if !fv.CanInterface() {
			err = errors.ServiceError("fns SQL: dao failed for get field interface value").WithMeta(field, "can not interface")
			return
		}
		fvv := fv.Interface()
		params.Append(fvv)
	}
	// do
	result, execErr := Execute(ctx, Param{
		Query: query,
		Args:  params,
	})
	if execErr != nil {
		err = execErr
		return
	}

	affected = int(result.Affected)
	if affected > 0 {
		d.affected(row)
		// version
		if info.Version != nil {
			rvv := rv.FieldByName(info.Version.StructFieldName)
			pre := rvv.Int()
			rvv.SetInt(pre + 1)
		}
		// fk
		for _, fc := range fcs {
			fcAffected, fcErr := d.deleteOne(ctx, fc)
			if fcErr != nil {
				err = errors.ServiceError("fns SQL: dao failed for foreign columns")
				return
			}
			affected = affected + fcAffected
		}
		// lk
		for _, lc := range lcs {
			lkAffected, lcErr := d.deleteOne(ctx, lc)
			if lcErr != nil {
				err = errors.ServiceError("fns SQL: dao failed for link columns")
				return
			}
			affected = affected + lkAffected
		}
	}
	d.Cache.Remove(row)
	return
}
