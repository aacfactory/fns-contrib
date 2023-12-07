package databases

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/logs"
	"sync/atomic"
	"time"
)

func Cluster() Database {
	return &cluster{}
}

type clusterConfig struct {
	Driver      string           `json:"driver"`
	DSN         []string         `json:"dsn"`
	MaxIdles    int              `json:"maxIdles"`
	MaxOpens    int              `json:"maxOpens"`
	MaxIdleTime time.Duration    `json:"maxIdleTime"`
	MaxLifetime time.Duration    `json:"maxLifetime"`
	Statements  StatementsConfig `json:"statements"`
}

type cluster struct {
	log        logs.Logger
	nodes      []*sql.DB
	nodesLen   uint32
	pos        uint32
	prepare    bool
	statements []*Statements
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
	if config.Statements.Enable {
		cacheSize := config.Statements.CacheSize
		if cacheSize < 1 {
			cacheSize = 1024
		}
		evictTimeoutSeconds := config.Statements.EvictTimeoutSeconds
		if evictTimeoutSeconds < 1 {
			evictTimeoutSeconds = 10
		}
		for _, node := range db.nodes {
			statements, stmtsErr := NewStatements(db.log, node, cacheSize, time.Duration(evictTimeoutSeconds)*time.Second)
			if stmtsErr != nil {
				err = errors.Warning("sql: cluster database construct failed").WithCause(stmtsErr)
				return
			}
			db.statements = append(db.statements, statements)
		}
		db.prepare = true
	}
	return
}

func (db *cluster) Begin(ctx context.Context, options TransactionOptions) (tx Transaction, err error) {
	pos := atomic.AddUint32(&db.pos, 1) % db.nodesLen
	core, begErr := db.nodes[pos].BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(options.Isolation),
		ReadOnly:  options.Readonly,
	})
	if begErr != nil {
		err = begErr
		return
	}
	if db.prepare {
		tx = &DefaultTransaction{
			core:       core,
			prepare:    true,
			statements: db.statements[pos],
		}
	} else {
		tx = &DefaultTransaction{
			core: core,
		}
	}
	return
}

func (db *cluster) Query(ctx context.Context, query []byte, args []any) (rows Rows, err error) {
	var r *sql.Rows
	pos := atomic.AddUint32(&db.pos, 1) % db.nodesLen
	if db.prepare {
		stmt, prepareErr := db.statements[pos].Get(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		r, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			if errors.Contains(err, ErrStatementClosed) {
				rows, err = db.Query(ctx, query, args)
				return
			}
			return
		}
	} else {
		r, err = db.nodes[pos].QueryContext(ctx, bytex.ToString(query), args...)
		if err != nil {
			return
		}
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (db *cluster) Execute(ctx context.Context, query []byte, args []any) (result Result, err error) {
	var r sql.Result
	pos := atomic.AddUint32(&db.pos, 1) % db.nodesLen
	if db.prepare {
		stmt, prepareErr := db.statements[pos].Get(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		r, err = stmt.ExecContext(ctx, args...)
		if err != nil {
			if errors.Contains(err, ErrStatementClosed) {
				result, err = db.Execute(ctx, query, args)
				return
			}
			return
		}
	} else {
		r, err = db.nodes[pos].ExecContext(ctx, bytex.ToString(query), args...)
		if err != nil {
			return
		}
	}
	rowsAffected, rowsAffectedErr := r.RowsAffected()
	if rowsAffectedErr != nil {
		err = rowsAffectedErr
		return
	}

	lastInsertId, lastInsertIdErr := r.LastInsertId()
	if lastInsertIdErr != nil {
		lastInsertId = -1
	}

	result = Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}
	return
}

func (db *cluster) Close(_ context.Context) (err error) {
	if db.prepare {
		for _, statements := range db.statements {
			statements.Close()
		}
	}
	errs := errors.MakeErrors()
	for _, node := range db.nodes {
		if closeErr := node.Close(); closeErr != nil {
			errs.Append(closeErr)
		}
	}
	if len(errs) > 0 {
		err = errors.Warning("sql: cluster database close failed").WithCause(errs.Error())
		return
	}
	return
}
