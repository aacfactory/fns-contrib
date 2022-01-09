package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
	"time"
)

func (d *dao) Update(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError) {
	if rows == nil || len(rows) == 0 {
		affected = 0
		return
	}
	txErr := d.beginTransaction(ctx)
	if txErr != nil {
		err = errors.ServiceError("fns SQL: dao begin tx failed").WithCause(txErr)
		return
	}

	for _, row := range rows {
		if row == nil {
			continue
		}
		affected0, err0 := d.updateOne(ctx, row)
		if err0 != nil {
			err = err0
			return
		}
		affected = affected + affected0
	}
	cmtErr := d.commitTransaction(ctx)
	if cmtErr != nil {
		err = errors.ServiceError("fns SQL: dao commit tx failed").WithCause(cmtErr)
		return
	}
	return
}

func (d *dao) updateOne(ctx fns.Context, row TableRow) (affected int, err errors.CodeError) {
	if d.hasAffected(row) {
		return
	}
	info := getTableRowInfo(row)
	query := info.UpdateQuery.Query
	paramFields := info.UpdateQuery.Params

	rt := reflect.TypeOf(row).Elem()
	rv := reflect.Indirect(reflect.ValueOf(row))

	fcs := make([]TableRow, 0, 1)
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
	// modify by
	if info.ModifyBY != nil {
		modifyBy := ctx.User().Id()
		if modifyBy != "" {
			rct, _ := rt.FieldByName(info.ModifyBY.StructFieldName)
			rvv := rv.FieldByName(info.ModifyBY.StructFieldName)
			switch rct.Type.Kind() {
			case reflect.String:
				rvv.SetString(modifyBy)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				userId, parseErr := strconv.Atoi(modifyBy)
				if parseErr == nil {
					rvv.SetInt(int64(userId))
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				userId, parseErr := strconv.Atoi(modifyBy)
				if parseErr == nil {
					rvv.SetUint(uint64(userId))
				}
			}
		}
	}
	// modify at
	if info.ModifyAT != nil {
		rvv := rv.FieldByName(info.ModifyAT.StructFieldName)
		rvv.Set(reflect.ValueOf(time.Now()))
	}
	for _, field := range paramFields {
		fv := rv.FieldByName(field)

		// fk
		if info.IsForeign(field) {
			if fv.IsNil() {
				params.Append(nil)
			} else {
				if !fv.CanInterface() {
					err = errors.ServiceError("fns SQL: dao failed for get field interface value").WithMeta(field, "can not interface")
					return
				}

				fvv := fv.Interface()
				fk := info.GetForeign(field)
				if fk.Sync {
					fcr, ok := fvv.(TableRow)
					if !ok {
						panic(fmt.Sprintf("fns SQL: use DAO failed for %s of row is not sql.TableRow", field))
					}
					fcs = append(fcs, fcr)
				}
				fkTableInfo := getTableRowInfo(fvv)
				fpk := fkTableInfo.Pks[0]
				ffv := fv.Elem().FieldByName(fpk.StructFieldName)
				params.Append(ffv.Interface())
			}
			continue
		}
		// json
		if info.IsJson(field) {
			ft, hasFt := rt.FieldByName(field)
			if !hasFt {
				continue
			}
			if ft.Type.Kind() == reflect.Ptr {
				if fv.IsNil() {
					params.Append([]byte("{}"))
				} else {
					fvi := fv.Elem().Interface()
					fvv, encodeErr := json.Marshal(fvi)
					if encodeErr != nil {
						err = errors.ServiceError("fns SQL: dao failed for json marshal").WithCause(encodeErr)
						return
					}
					params.Append(fvv)
				}
			} else if ft.Type.Kind() == reflect.Slice || ft.Type.Kind() == reflect.Array {
				if fv.Len() == 0 {
					params.Append([]byte("[]"))
				} else {
					fvi := fv.Elem().Interface()
					fvv, encodeErr := json.Marshal(fvi)
					if encodeErr != nil {
						err = errors.ServiceError("fns SQL: dao failed for json marshal").WithCause(encodeErr)
						return
					}
					params.Append(fvv)
				}
			}
			continue
		}
		// builtin
		if !fv.CanInterface() {
			err = errors.ServiceError("fns SQL: dao insert for get field interface value").WithMeta(field, "can not interface")
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
			fcAffected, fcErr := d.updateOne(ctx, fc)
			if fcErr != nil {
				err = errors.ServiceError("fns SQL: dao failed for foreign columns")
				return
			}
			affected = affected + fcAffected
		}
		// lk
		for _, lc := range lcs {
			lkAffected, lcErr := d.updateOne(ctx, lc)
			if lcErr != nil {
				err = errors.ServiceError("fns SQL: dao failed for link columns")
				return
			}
			affected = affected + lkAffected
		}
	}
	return
}
