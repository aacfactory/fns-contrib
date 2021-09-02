package sql

import (
	"context"
	db "database/sql"
	"fmt"
	"github.com/aacfactory/fns/commons"
)

type Client interface {
	Reader() (v *db.DB)
	Writer() (v *db.DB)
	Close() (err error)
}

type QueryAble interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*db.Rows, error)
}

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (db.Result, error)
}

// +-------------------------------------------------------------------------------------------------------------------+

func NewStandalone(v *db.DB) (client *Standalone) {
	client = &Standalone{
		v: v,
	}
	return
}

type Standalone struct {
	v *db.DB
}

func (client *Standalone) Reader() (v *db.DB) {
	v = client.v
	return
}

func (client *Standalone) Writer() (v *db.DB) {
	v = client.v
	return
}

func (client *Standalone) Close() (err error) {
	err = client.v.Close()
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

type KDB struct {
	key string
	v   *db.DB
}

func (k *KDB) Key() string {
	return k.key
}

// +-------------------------------------------------------------------------------------------------------------------+

func NewMasterSlaver(master *db.DB, slavers []*db.DB) (client *MasterSlaver) {
	client = &MasterSlaver{
		master:  master,
		slavers: commons.NewRing(),
	}
	for i, d := range slavers {
		client.slavers.Append(&KDB{
			key: fmt.Sprintf("%d", i),
			v:   d,
		})
	}
	return
}

type MasterSlaver struct {
	master  *db.DB
	slavers *commons.Ring
}

func (client *MasterSlaver) Reader() (v *db.DB) {
	x := client.slavers.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*KDB)
	v = kdb.v
	return
}

func (client *MasterSlaver) Writer() (v *db.DB) {
	v = client.master
	return
}

func (client *MasterSlaver) Close() (err error) {
	masterErr := client.master.Close()
	if masterErr != nil {
		err = masterErr
	}
	for i := 0; i < client.slavers.Size(); i++ {
		x := client.slavers.Next()
		if x == nil {
			return
		}
		kdb, _ := x.(*KDB)
		slaverErr := kdb.v.Close()
		if slaverErr != nil {
			if err == nil {
				err = slaverErr
			} else {
				err = fmt.Errorf("%v, %v", err, slaverErr)
			}
		}
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

func NewCluster(databases []*db.DB) (client *Cluster) {
	client = &Cluster{
		dbs: commons.NewRing(),
	}
	for i, d := range databases {
		client.dbs.Append(&KDB{
			key: fmt.Sprintf("%d", i),
			v:   d,
		})
	}
	return
}

type Cluster struct {
	dbs *commons.Ring
}

func (client *Cluster) Reader() (v *db.DB) {
	x := client.dbs.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*KDB)
	v = kdb.v
	return
}

func (client *Cluster) Writer() (v *db.DB) {
	x := client.dbs.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*KDB)
	v = kdb.v
	return
}

func (client *Cluster) Close() (err error) {
	for i := 0; i < client.dbs.Size(); i++ {
		x := client.dbs.Next()
		if x == nil {
			return
		}
		kdb, _ := x.(*KDB)
		slaverErr := kdb.v.Close()
		if slaverErr != nil {
			if err == nil {
				err = slaverErr
			} else {
				err = fmt.Errorf("%v, %v", err, slaverErr)
			}
		}
	}
	return
}
