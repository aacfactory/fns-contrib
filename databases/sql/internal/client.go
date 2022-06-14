package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/fns/commons/container/ring"
	"runtime"
	"strings"
)

type Client interface {
	Reader() (v *sql.DB)
	Writer() (v *sql.DB)
	Close() (err error)
}

type QueryAble interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error)
}

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error)
}

func newClient(config Config) (client Client, err error) {
	if config.DSN == nil || len(config.DSN) < 1 {
		err = fmt.Errorf("sql: dsn is invalid")
		return
	}
	driver := strings.TrimSpace(config.Driver)
	if driver == "" {
		err = fmt.Errorf("sql: driver is invalid")
		return
	}
	maxIdles := config.MaxIdles
	if maxIdles < 1 {
		maxIdles = 1
	}
	maxOpens := config.MaxOpens
	if maxOpens < 1 {
		maxOpens = runtime.NumCPU() * 2
	}

	if config.MasterSlaverMode {
		if len(config.DSN) < 2 {
			err = fmt.Errorf("sql: masterSlaverMode is enabled but num of dsn is not gt 1")
			return
		}
		masterDSN := strings.TrimSpace(config.DSN[0])
		if masterDSN == "" {
			err = fmt.Errorf("sql: masterSlaverMode is enabled but first of dsn is empty")
			return
		}
		master, openMasterErr := sql.Open(driver, masterDSN)
		if openMasterErr != nil {
			err = fmt.Errorf("sql: create master failed, dsn is %s, %v", masterDSN, openMasterErr)
			return
		}
		master.SetMaxIdleConns(maxIdles)
		master.SetMaxOpenConns(maxOpens)
		pingErr := master.PingContext(context.TODO())
		if pingErr != nil {
			err = fmt.Errorf("sql: ping %s failed, %v", masterDSN, pingErr)
			return
		}

		slavers := make([]*sql.DB, 0, 1)

		slaverDSNs := config.DSN[1:]
		for _, slaverDSN := range slaverDSNs {
			slaverDSN = strings.TrimSpace(slaverDSN)
			if slaverDSN == "" {
				err = fmt.Errorf("sql: masterSlaverMode is enabled but one of slaver dsns is empty")
				return
			}
			slaver, openSlaverErr := sql.Open(driver, slaverDSN)
			if openSlaverErr != nil {
				err = fmt.Errorf("sql: create slaver failed, dsn is %s, %v", slaverDSN, openSlaverErr)
				return
			}
			slaver.SetMaxIdleConns(maxIdles)
			slaver.SetMaxOpenConns(maxOpens)
			pingSlaverErr := slaver.PingContext(context.TODO())
			if pingSlaverErr != nil {
				err = fmt.Errorf("sql: ping %s failed, %v", slaverDSN, pingSlaverErr)
				return
			}
			slavers = append(slavers, slaver)
		}

		client = newMasterSlaver(master, slavers)

	} else {
		if len(config.DSN) == 1 {
			dsn := strings.TrimSpace(config.DSN[0])
			if dsn == "" {
				err = fmt.Errorf("sql: dsn is empty")
				return
			}
			d, openErr := sql.Open(driver, dsn)
			if openErr != nil {
				err = fmt.Errorf("sql: create connection failed, dsn is %s, %v", dsn, openErr)
				return
			}
			d.SetMaxIdleConns(maxIdles)
			d.SetMaxOpenConns(maxOpens)
			pingErr := d.PingContext(context.TODO())
			if pingErr != nil {
				err = fmt.Errorf("sql: ping %s failed, %v", dsn, pingErr)
				return
			}
			client = newStandalone(d)
		} else {
			v := make([]*sql.DB, 0, 1)
			for _, dsn := range config.DSN {
				dsn = strings.TrimSpace(dsn)
				if dsn == "" {
					err = fmt.Errorf("sql: dsn is empty")
					return
				}
				d, openErr := sql.Open(driver, dsn)
				if openErr != nil {
					err = fmt.Errorf("sql: create connection failed, dsn is %s, %v", dsn, openErr)
					return
				}
				d.SetMaxIdleConns(maxIdles)
				d.SetMaxOpenConns(maxOpens)
				pingErr := d.PingContext(context.TODO())
				if pingErr != nil {
					err = fmt.Errorf("sql: ping %s failed, %v", dsn, pingErr)
					return
				}
				v = append(v, d)
			}
			client = newCluster(v)
		}
	}
	return
}

func newStandalone(v *sql.DB) (client Client) {
	client = &standalone{
		v: v,
	}
	return
}

type standalone struct {
	v *sql.DB
}

func (client *standalone) Reader() (v *sql.DB) {
	v = client.v
	return
}

func (client *standalone) Writer() (v *sql.DB) {
	v = client.v
	return
}

func (client *standalone) Close() (err error) {
	err = client.v.Close()
	return
}

type keyedClient struct {
	key string
	v   *sql.DB
}

func (k *keyedClient) Key() string {
	return k.key
}

func newMasterSlaver(master *sql.DB, slavers []*sql.DB) (client Client) {
	slaversRing := ring.New()
	for i, d := range slavers {
		slaversRing.Append(&keyedClient{
			key: fmt.Sprintf("%d", i),
			v:   d,
		})
	}
	client = &masterSlavered{
		master:  master,
		slavers: slaversRing,
	}
	return
}

type masterSlavered struct {
	master  *sql.DB
	slavers *ring.Ring
}

func (client *masterSlavered) Reader() (v *sql.DB) {
	x := client.slavers.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*keyedClient)
	v = kdb.v
	return
}

func (client *masterSlavered) Writer() (v *sql.DB) {
	v = client.master
	return
}

func (client *masterSlavered) Close() (err error) {
	masterErr := client.master.Close()
	if masterErr != nil {
		err = masterErr
	}
	for i := 0; i < client.slavers.Size(); i++ {
		x := client.slavers.Next()
		if x == nil {
			return
		}
		kdb, _ := x.(*keyedClient)
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

func newCluster(databases []*sql.DB) (client Client) {
	dbs := ring.New()
	for i, d := range databases {
		dbs.Append(&keyedClient{
			key: fmt.Sprintf("%d", i),
			v:   d,
		})
	}
	client = &cluster{
		dbs: dbs,
	}
	return
}

type cluster struct {
	dbs *ring.Ring
}

func (client *cluster) Reader() (v *sql.DB) {
	x := client.dbs.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*keyedClient)
	v = kdb.v
	return
}

func (client *cluster) Writer() (v *sql.DB) {
	x := client.dbs.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*keyedClient)
	v = kdb.v
	return
}

func (client *cluster) Close() (err error) {
	for i := 0; i < client.dbs.Size(); i++ {
		x := client.dbs.Next()
		if x == nil {
			return
		}
		kdb, _ := x.(*keyedClient)
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
