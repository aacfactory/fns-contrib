package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
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