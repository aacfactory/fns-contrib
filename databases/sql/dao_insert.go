package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
	"time"
)

func (d *dao) Insert(ctx fns.Context) (affected int, err errors.CodeError) {
	// tx begin
	txErr := d.beginTx(ctx)
	if txErr != nil {
		err = errors.ServiceError("fns SQL: dao begin tx failed").WithCause(txErr)
		return
	}
	if d.TargetIsArray {
		affected, err = d.insertArray(ctx)
	} else {
		affected, err = d.insertOne(ctx)
	}
	if err != nil {
		return
	}
	// tx commit
	cmtErr := d.commitTx(ctx)
	if cmtErr != nil {
		err = errors.ServiceError("fns SQL: dao commit tx failed").WithCause(cmtErr)
		return
	}
	return
}

func (d *dao) insertOne(ctx fns.Context) (affected int, err errors.CodeError) {

	rt := reflect.TypeOf(d.Target).Elem()
	rv := reflect.Indirect(reflect.ValueOf(d.Target))
	pks := make([]interface{}, 0, 1)
	for _, pk := range d.TableInfo.Pks {
		pks = append(pks, rv.FieldByName(pk.StructFieldName).Interface())
	}
	if d.hasAffected(pks) {
		return
	}

	query := d.TableInfo.InsertQuery.Query
	paramFields := d.TableInfo.InsertQuery.Params

	fcs := make([]interface{}, 0, 1)
	lcs := make([]interface{}, 0, 1)
	for _, column := range d.TableInfo.LinkColumns {
		if column.Sync {
			fv := rv.FieldByName(column.StructFieldName)
			if fv.Len() > 0 {
				for i := 0; i < fv.Len(); i++ {
					lcs = append(lcs, fv.Index(i).Interface())
				}
			}
		}
	}

	params := NewTuple()

	// create by
	if d.TableInfo.CreateBY != nil {
		createBy := ctx.User().Id()
		if createBy != "" {
			rct, _ := rt.FieldByName(d.TableInfo.CreateBY.StructFieldName)
			rvv := rv.FieldByName(d.TableInfo.CreateBY.StructFieldName)
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
	if d.TableInfo.CreateAT != nil {
		rvv := rv.FieldByName(d.TableInfo.CreateAT.StructFieldName)
		rvv.Set(reflect.ValueOf(time.Now()))
	}

	for _, field := range paramFields {
		fv := rv.FieldByName(field)

		// fk
		if d.TableInfo.IsForeign(field) {
			if fv.IsNil() {
				params.Append(nil)
			} else {
				if !fv.CanInterface() {
					err = errors.ServiceError("fns SQL: dao insert failed for get field interface value").WithMeta(field, "can not interface")
					return
				}

				fvv := fv.Interface()
				fk := d.TableInfo.GetForeign(field)
				if fk.Sync {
					fcs = append(fcs, fvv)
				}
				fkTableInfo := newTableInfo(fvv, d.Driver)
				fpk := fkTableInfo.Pks[0]
				ffv := fv.Elem().FieldByName(fpk.StructFieldName)
				params.Append(ffv.Interface())
			}
			continue
		}
		// json
		if d.TableInfo.IsJson(field) {
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
						err = errors.ServiceError("fns SQL: dao insert failed for json marshal").WithCause(encodeErr)
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
						err = errors.ServiceError("fns SQL: dao insert failed for json marshal").WithCause(encodeErr)
						return
					}
					params.Append(fvv)
				}
			}
			continue
		}
		// builtin
		if !fv.CanInterface() {
			err = errors.ServiceError("fns SQL: dao insert failed for get field interface value").WithMeta(field, "can not interface")
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
		rv.FieldByName(d.TableInfo.Pks[0].StructFieldName).SetInt(result.LastInsertId)
	}
	affected = int(result.Affected)
	if affected > 0 {
		d.affected(pks)
		// version
		if d.TableInfo.Version != nil {
			rvv := rv.FieldByName(d.TableInfo.Version.StructFieldName)
			pre := rvv.Int()
			rvv.SetInt(pre + 1)
		}
		// fk
		for _, fc := range fcs {
			fd := newDAO(fc, d.Loaded, d.Affected)
			fcAffected, fcErr := fd.insertOne(ctx)
			if fcErr != nil {
				err = errors.ServiceError("fns SQL: dao insert failed for foreign columns")
				return
			}
			affected = affected + fcAffected
		}
		// lk
		for _, lc := range lcs {
			ld := newDAO(lc, d.Loaded, d.Affected)
			lkAffected, lcErr := ld.insertOne(ctx)
			if lcErr != nil {
				err = errors.ServiceError("fns SQL: dao insert failed for link columns")
				return
			}
			affected = affected + lkAffected
		}
	}
	return
}

func (d *dao) insertArray(ctx fns.Context) (affected int, err errors.CodeError) {
	rv := reflect.Indirect(reflect.ValueOf(d.Target))
	size := rv.Len()
	if size < 1 {
		return
	}
	for i := 0; i < size; i++ {
		v := rv.Index(i)
		if v.IsNil() {
			continue
		}
		x := v.Interface()
		xDAO := newDAO(x, d.Loaded, d.Affected)
		xAffected, xErr := xDAO.insertOne(ctx)
		if xErr != nil {
			err = xErr
			return
		}
		affected = affected + xAffected
	}
	return
}
