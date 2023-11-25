package databases

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"sync/atomic"
	"time"
	"unsafe"
)

func Cluster() Database {
	return &cluster{}
}

type clusterConfig struct {
	Driver      string        `json:"driver"`
	DSN         []string      `json:"dsn"`
	MaxIdles    int           `json:"maxIdles"`
	MaxOpens    int           `json:"maxOpens"`
	MaxIdleTime time.Duration `json:"maxIdleTime"`
	MaxLifetime time.Duration `json:"maxLifetime"`
}

type cluster struct {
	log      logs.Logger
	nodes    []*sql.DB
	nodesLen uint32
	pos      uint32
}

func (db *cluster) Name() string {
	return "cluster"
}

func (db *cluster) Construct(options Options) (err error) {
	db.log = options.Log
	config := clusterConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("sql: cluster database construct failed").WithCause(configErr)
		return
	}
	db.nodes = make([]*sql.DB, 0, len(config.DSN))
	for _, dsn := range config.DSN {
		node, nodeErr := sql.Open(config.Driver, dsn)
		if nodeErr != nil {
			err = errors.Warning("sql: cluster database construct failed").WithCause(nodeErr)
			return
		}
		maxIdles := config.MaxIdles
		if maxIdles > 0 {
			node.SetMaxIdleConns(maxIdles)
		}
		maxOpens := config.MaxOpens
		if maxOpens > 0 {
			node.SetMaxOpenConns(maxOpens)
		}
		maxIdleTime := config.MaxIdleTime
		if maxIdleTime > 0 {
			node.SetConnMaxIdleTime(maxIdleTime)
		}
		maxLifetime := config.MaxLifetime
		if maxLifetime > 0 {
			node.SetConnMaxLifetime(maxLifetime)
		}
		err = node.Ping()
		if err != nil {
			err = errors.Warning("sql: cluster database construct failed").WithCause(err)
			return
		}
		db.nodes = append(db.nodes, node)
	}
	db.nodesLen = uint32(len(db.nodes))
	if db.nodesLen == 0 {
		err = errors.Warning("sql: cluster database construct failed").WithCause(fmt.Errorf("no slavers"))
		return
	}
	return
}

func (db *cluster) next() (node *sql.DB) {
	pos := atomic.AddUint32(&db.pos, 1)
	node = db.nodes[pos%db.nodesLen]
	return
}

func (db *cluster) Begin(ctx context.Context, options TransactionOptions) (tx Transaction, err error) {
	core, begErr := db.next().BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(options.Isolation),
		ReadOnly:  options.Readonly,
	})
	if begErr != nil {
		err = begErr
		return
	}
	tx = &DefaultTransaction{
		core: core,
	}
	return
}

func (db *cluster) Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error) {
	r, queryErr := db.next().QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
	if queryErr != nil {
		err = queryErr
		return
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (db *cluster) Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error) {
	r, execErr := db.next().ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
	if execErr != nil {
		err = execErr
		return
	}
	lastInsertId, lastInsertIdErr := r.LastInsertId()
	if lastInsertIdErr != nil {
		err = lastInsertIdErr
		return
	}
	rowsAffected, rowsAffectedErr := r.RowsAffected()
	if rowsAffectedErr != nil {
		err = rowsAffectedErr
		return
	}
	result = Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}
	return
}

func (db *cluster) Close(_ context.Context) (err error) {
	errs := errors.MakeErrors()
	for _, slaver := range db.nodes {
		if closeErr := slaver.Close(); closeErr != nil {
			errs.Append(closeErr)
		}
	}
	if len(errs) > 0 {
		err = errors.Warning("sql: cluster database close failed").WithCause(errs.Error())
		return
	}
	return
}
