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

type DatabaseAccessObject interface {
	Save(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	Insert(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	Update(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	Delete(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	Exist(ctx fns.Context, row TableRow) (has bool, err errors.CodeError)
	Get(ctx fns.Context, row TableRow) (has bool, err errors.CodeError)
	Query(ctx fns.Context, param *QueryParam, rows interface{}) (has bool, err errors.CodeError)
	Count(ctx fns.Context, param *QueryParam, row TableRow) (num int, err errors.CodeError)
	Page(ctx fns.Context, param *QueryParam, rows interface{}) (page Paged, err errors.CodeError)
	Close()
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	dialect         = ""
	dialectLoadOnce = sync.Once{}
	tableInfoMap    = sync.Map{}
)

func RegisterDialect(name string) {
	if name != "mysql" && name != "postgres" {
		panic(fmt.Sprintf("fns SQL: use DAO failed for %s dialect is not supported", name))
	}
	dialect = name
	if dialect == "pgx" {
		dialect = "postgres"
	}
}

func DAO(ctx fns.Context) (v DatabaseAccessObject) {
	dialectLoadOnce.Do(func() {
		if dialect == "" {
			drivers := db.Drivers()
			if drivers == nil || len(drivers) != 1 {
				panic("fns SQL: use DAO failed for no drivers or too many drivers")
			}
			dialect = drivers[0]
			if dialect == "pgx" {
				dialect = "postgres"
			}
			if dialect != "postgres" && dialect != "mysql" {
				panic(fmt.Sprintf("fns SQL: use DAO failed for %s driver is not supported", dialect))
			}
		}
	})
	v = &dao{
		Cache:    getDAOCache(ctx),
		Affected: sync.Map{},
	}
	return
}

type dao struct {
	Cache    DaoCache
	Affected sync.Map
}

func (d *dao) hasAffected(row interface{}) (has bool) {
	info := getTableRowInfo(row)
	rv := reflect.Indirect(reflect.ValueOf(row))
	pks := make([]interface{}, 0, 1)
	for _, pk := range info.Pks {
		pks = append(pks, rv.FieldByName(pk.StructFieldName).Interface())
	}
	rt := reflect.TypeOf(row)
	key := fmt.Sprintf("%s:%s", rt.PkgPath(), rt.Name())
	for _, value := range pks {
		key = key + "," + fmt.Sprintf("%v", value)
	}
	_, has = d.Affected.Load(key)
	return
}

func (d *dao) affected(row interface{}) {
	info := getTableRowInfo(row)
	rv := reflect.Indirect(reflect.ValueOf(row))
	pks := make([]interface{}, 0, 1)
	for _, pk := range info.Pks {
		pks = append(pks, rv.FieldByName(pk.StructFieldName).Interface())
	}
	rt := reflect.TypeOf(row)
	key := fmt.Sprintf("%s:%s", rt.PkgPath(), rt.Name())
	for _, value := range pks {
		key = key + "," + fmt.Sprintf("%v", value)
	}
	d.Affected.Store(key, 1)
	return
}

func (d *dao) affectedClean() {
	keys := make([]interface{}, 0, 1)
	d.Affected.Range(func(key, value interface{}) bool {
		keys = append(keys, key)
		return true
	})
	for _, key := range keys {
		d.Affected.Delete(key)
	}
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

func (d *dao) Close() {
	d.Cache.Clean()
	d.affectedClean()
}
