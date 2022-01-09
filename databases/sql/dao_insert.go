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

func (d *dao) Insert(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError) {
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
		affected0, err0 := d.insertOne(ctx, row)
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

func (d *dao) insertOne(ctx fns.Context, row TableRow) (affected int, err errors.CodeError) {
	if d.hasAffected(row) {
		return
	}
	info := getTableRowInfo(row)

	query := info.InsertQuery.Query
	paramFields := info.InsertQuery.Params

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

	// create by
	if info.CreateBY != nil {
		createBy := ctx.User().Id()
		if createBy != "" {
			rct, _ := rt.FieldByName(info.CreateBY.StructFieldName)
			rvv := rv.FieldByName(info.CreateBY.StructFieldName)
			switch rct.Type.Kind() {
			case reflect.String:
				rvv.SetString(createBy)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				userId, parseErr := strconv.Atoi(createBy)
				if parseErr == nil {
					rvv.SetInt(int64(userId))
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				userId, parseErr := strconv.Atoi(createBy)
				if parseErr == nil {
					rvv.SetUint(uint64(userId))
				}
			}
		}
	}
	// create at
	if info.CreateAT != nil {
		rvv := rv.FieldByName(info.CreateAT.StructFieldName)
		rvv.Set(reflect.ValueOf(time.Now()))
	}
	// version
	version := int64(0)
	if info.Version != nil {
		rvv := rv.FieldByName(info.Version.StructFieldName)
		version = rvv.Int()
		if version < 1 {
			version++
		}
	}

	for _, field := range paramFields {
		fv := rv.FieldByName(field)
		if field == info.Version.StructFieldName {
			params.Append(version)
			continue
		}
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
	if result.LastInsertId > 0 {
		rv.FieldByName(info.Pks[0].StructFieldName).SetInt(result.LastInsertId)
	}
	affected = int(result.Affected)
	if affected > 0 {
		d.affected(row)
		// version
		if info.Version != nil {
			rvv := rv.FieldByName(info.Version.StructFieldName)
			rvv.SetInt(version)
		}
		// fk
		for _, fc := range fcs {
			fcAffected, fcErr := d.insertOne(ctx, fc)
			if fcErr != nil {
				err = errors.ServiceError("fns SQL: dao failed for foreign columns")
				return
			}
			affected = affected + fcAffected
		}
		// lk
		for _, lc := range lcs {
			lkAffected, lcErr := d.insertOne(ctx, lc)
			if lcErr != nil {
				err = errors.ServiceError("fns SQL: dao failed for link columns")
				return
			}
			affected = affected + lkAffected
		}
	}
	return
}
