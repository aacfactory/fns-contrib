package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/rings"
	"net/url"
	"runtime"
	"strings"
)

type Client interface {
	SchemaOfDSN() (schema string)
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

func newClient(config *Config) (client Client, err error) {
	if config.DSN == nil || len(config.DSN) < 1 {
		err = fmt.Errorf("sql: dsn is invalid")
		return
	}
	dsnURL, parseDSNErr := url.Parse(config.DSN[0])
	if parseDSNErr != nil {
		err = fmt.Errorf("sql: parse dsn failed, %v", parseDSNErr)
		return
	}
	schemaOfDSN := dsnURL.Scheme
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

		client = newMasterSlaver(schemaOfDSN, master, slavers)

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
			client = newStandalone(schemaOfDSN, d)
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
			client = newCluster(schemaOfDSN, v)
		}
	}
	return
}

func newStandalone(schemaOfDSN string, v *sql.DB) (client Client) {
	client = &standalone{
		v:           v,
		schemaOfDSN: schemaOfDSN,
	}
	return
}

type standalone struct {
	v           *sql.DB
	schemaOfDSN string
}

func (client *standalone) SchemaOfDSN() (schema string) {
	schema = client.schemaOfDSN
	return
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

func newMasterSlaver(schemaOfDSN string, master *sql.DB, slavers []*sql.DB) (client Client) {
	slaversRing := rings.New[*keyedClient]("slavers")
	for i, d := range slavers {
		slaversRing.Push(&keyedClient{
			key: fmt.Sprintf("%d", i),
			v:   d,
		})
	}
	client = &masterSlavered{
		master:      master,
		slavers:     slaversRing,
		schemaOfDSN: schemaOfDSN,
	}
	return
}

type masterSlavered struct {
	master      *sql.DB
	slavers     *rings.Ring[*keyedClient]
	schemaOfDSN string
}

func (client *masterSlavered) SchemaOfDSN() (schema string) {
	schema = client.schemaOfDSN
	return
}

func (client *masterSlavered) Reader() (v *sql.DB) {
	kdb := client.slavers.Next()
	if kdb == nil {
		return
	}
	v = kdb.v
	return
}

func (client *masterSlavered) Writer() (v *sql.DB) {
	v = client.master
	return
}

func (client *masterSlavered) Close() (err error) {
	errs := errors.MakeErrors()
	masterErr := client.master.Close()
	if masterErr != nil {
		errs.Append(masterErr)
	}
	for i := 0; i < client.slavers.Len(); i++ {
		kdb := client.slavers.Next()
		closeErr := kdb.v.Close()
		if closeErr != nil {
			errs.Append(closeErr)
		}
	}
	err = errs.Error()
	return
}

func newCluster(schemaOfDSN string, databases []*sql.DB) (client Client) {
	dbs := rings.New[*keyedClient]("cluster")
	for i, d := range databases {
		dbs.Push(&keyedClient{
			key: fmt.Sprintf("%d", i),
			v:   d,
		})
	}
	client = &cluster{
		dbs:         dbs,
		schemaOfDSN: schemaOfDSN,
	}
	return
}

type cluster struct {
	dbs         *rings.Ring[*keyedClient]
	schemaOfDSN string
}

func (client *cluster) SchemaOfDSN() (schema string) {
	schema = client.schemaOfDSN
	return
}

func (client *cluster) Reader() (v *sql.DB) {
	kdb := client.dbs.Next()
	if kdb == nil {
		return
	}
	v = kdb.v
	return
}

func (client *cluster) Writer() (v *sql.DB) {
	kdb := client.dbs.Next()
	if kdb == nil {
		return
	}
	v = kdb.v
	return
}

func (client *cluster) Close() (err error) {
	errs := errors.MakeErrors()
	for i := 0; i < client.dbs.Len(); i++ {
		kdb := client.dbs.Next()
		closeErr := kdb.v.Close()
		if closeErr != nil {
			errs.Append(closeErr)
		}
	}
	err = errs.Error()
	return
}
