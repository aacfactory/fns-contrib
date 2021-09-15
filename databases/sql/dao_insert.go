package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"reflect"
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

	params := NewTuple()

	for _, field := range paramFields {
		fv := rv.FieldByName(field)
		// lk
		if d.TableInfo.IsLink(field) {
			lk := d.TableInfo.GetLink(field)
			if lk.Sync {
				if fv.Len() > 0 {
					fvv := fv.Interface()
					lcs = append(lcs, &fvv)
				}
			}
			continue
		}
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
				ffv := fv.FieldByName(fpk.StructFieldName)
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
					fvi := fv.Interface()
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
					fvi := fv.Interface()
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
	d.affected(pks)

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
		lkAffected, lcErr := ld.insertArray(ctx)
		if lcErr != nil {
			err = errors.ServiceError("fns SQL: dao insert failed for link columns")
			return
		}
		affected = affected + lkAffected
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
