package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"reflect"
	"sync"
	"time"
)

type TableRow interface {
	Table() (namespace string, name string, alias string)
}

type QueryParam struct {
	Condition string
	Sorts     string
	params    *Tuple
}

type Paged struct {
	No    int
	Num   int // int(math.Ceil(float64(total)/float64(pageSize)))
	Total int
}

type DatabaseAccessObject interface {
	Insert(ctx fns.Context) (affected int, err errors.CodeError)
	Update(ctx fns.Context) (affected int, err errors.CodeError)
	Delete(ctx fns.Context) (affected int, err errors.CodeError)
	Exist(ctx fns.Context) (has bool, err errors.CodeError)
	Get(ctx fns.Context) (has bool, err errors.CodeError)
	Query(ctx fns.Context, param QueryParam, offset int, length int) (has bool, err errors.CodeError)
	Count(ctx fns.Context, param QueryParam) (num int, err errors.CodeError)
	Page(ctx fns.Context, param QueryParam, pageNo int, pageSize int) (has Paged, err errors.CodeError)
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	driver         = ""
	driverLoadOnce = sync.Once{}
	tableInfoMap   = sync.Map{}
)

func DAO(target interface{}) (v DatabaseAccessObject) {
	driverLoadOnce.Do(func() {
		drivers := db.Drivers()
		if drivers == nil || len(drivers) != 1 {
			panic("fns SQL: use DAO failed for no drivers or too many drivers")
		}
		driver = drivers[0]
		if driver != "postgres" && driver != "mysql" {
			panic(fmt.Sprintf("fns SQL: use DAO failed for %s driver is not supported", driver))
		}
	})
	v = newDAO(target, make(map[string]interface{}), make(map[string]bool))
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

func newDAO(target interface{}, loaded map[string]interface{}, affected map[string]bool) (v *dao) {
	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("fns SQL: use DAO failed for target must be ptr"))
	}
	targetIsArray := false
	var info *tableInfo
	rt = rt.Elem()
	if rt.Kind() == reflect.Struct {
		targetIsArray = false
		info = newTableInfo(target, driver)
	} else if rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
		rt = rt.Elem()
		targetIsArray = true
		if rt.Kind() != reflect.Ptr {
			panic(fmt.Sprintf("fns SQL: use DAO failed for element of slice target must be ptr struct"))
		}
		if rt.Elem().Kind() != reflect.Struct {
			panic(fmt.Sprintf("fns SQL: use DAO failed for element of slice target must be ptr struct"))
		}
		x := reflect.New(rt.Elem()).Interface()
		info = newTableInfo(x, driver)
	} else {
		panic(fmt.Sprintf("fns SQL: use DAO failed for element of target must be struct of slice"))
	}
	v = &dao{
		Driver:        driver,
		Target:        target,
		TargetIsArray: targetIsArray,
		TableInfo:     info,
		Loaded:        loaded,
		Affected:      affected,
	}
	return
}

type dao struct {
	Driver        string
	Target        interface{}
	TargetIsArray bool
	TableInfo     *tableInfo
	Loaded        map[string]interface{}
	Affected      map[string]bool
}

func (d *dao) buildKeyOfPK(pkValues []interface{}) (key string) {
	for i, value := range pkValues {
		if i == 0 {
			key = fmt.Sprintf("%v", value)
		} else {
			key = key + "," + fmt.Sprintf("%v", value)
		}
	}
	return
}

func (d *dao) getLoaded(pkValues []interface{}) (v interface{}, has bool) {
	v, has = d.Loaded[d.buildKeyOfPK(pkValues)]
	return
}

func (d *dao) setLoaded(pkValues []interface{}, v interface{}) {
	d.Loaded[d.buildKeyOfPK(pkValues)] = v
	return
}

func (d *dao) hasAffected(pkValues []interface{}) (has bool) {
	_, has = d.Affected[d.buildKeyOfPK(pkValues)]
	return
}

func (d *dao) affected(pkValues []interface{}) {
	d.Affected[d.buildKeyOfPK(pkValues)] = true
	return
}

func (d *dao) beginTx(ctx fns.Context) (err errors.CodeError) {
	err = TxBegin(ctx, TxBeginParam{
		Timeout:   2 * time.Second,
		Isolation: 0,
	})
	return
}

func (d *dao) commitTx(ctx fns.Context) (err errors.CodeError) {
	err = TxCommit(ctx)
	return
}

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

func (d *dao) Update(ctx fns.Context) (affected int, err errors.CodeError) {
	panic("implement me")
}

func (d *dao) Delete(ctx fns.Context) (affected int, err errors.CodeError) {
	panic("implement me")
}

func (d *dao) Exist(ctx fns.Context) (has bool, err errors.CodeError) {

	return
}

func (d *dao) Get(ctx fns.Context) (has bool, err errors.CodeError) {
	panic("implement me")
}

// select * from test a inner join (select id from test where val=4 limit 300000,5) b on a.id=b.id;
func (d *dao) Query(ctx fns.Context, param QueryParam, offset int, length int) (has bool, err errors.CodeError) {
	panic("implement me")
}

func (d *dao) Count(ctx fns.Context, param QueryParam) (num int, err errors.CodeError) {
	panic("implement me")
}

func (d *dao) Page(ctx fns.Context, param QueryParam, pageNo int, pageSize int) (has Paged, err errors.CodeError) {
	panic("implement me")
}
