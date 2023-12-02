package databases

import (
	"context"
	"database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"time"
	"unsafe"
)

func Standalone() Database {
	return &standalone{}
}

type standaloneConfig struct {
	Driver      string           `json:"driver"`
	DSN         string           `json:"dsn"`
	MaxIdles    int              `json:"maxIdles"`
	MaxOpens    int              `json:"maxOpens"`
	MaxIdleTime time.Duration    `json:"maxIdleTime"`
	MaxLifetime time.Duration    `json:"maxLifetime"`
	Statements  StatementsConfig `json:"statements"`
}

type standalone struct {
	log        logs.Logger
	core       *sql.DB
	prepare    bool
	statements *Statements
}

func (db *standalone) Name() string {
	return "standalone"
}

func (db *standalone) Construct(options Options) (err error) {
	db.log = options.Log
	config := standaloneConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("sql: standalone database construct failed").WithCause(configErr)
		return
	}
	db.core, err = sql.Open(config.Driver, config.DSN)
	if err != nil {
		err = errors.Warning("sql: standalone database construct failed").WithCause(err)
		return
	}
	maxIdles := config.MaxIdles
	if maxIdles > 0 {
		db.core.SetMaxIdleConns(maxIdles)
	}
	maxOpens := config.MaxOpens
	if maxOpens > 0 {
		db.core.SetMaxOpenConns(maxOpens)
	}
	maxIdleTime := config.MaxIdleTime
	if maxIdleTime > 0 {
		db.core.SetConnMaxIdleTime(maxIdleTime)
	}
	maxLifetime := config.MaxLifetime
	if maxLifetime > 0 {
		db.core.SetConnMaxLifetime(maxLifetime)
	}
	err = db.core.Ping()
	if err != nil {
		err = errors.Warning("sql: standalone database construct failed").WithCause(err)
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
		db.statements, err = NewStatements(db.log, db.core, cacheSize, time.Duration(evictTimeoutSeconds)*time.Second)
		if err != nil {
			err = errors.Warning("sql: standalone database construct failed").WithCause(err)
			return
		}
		db.prepare = true
	}
	db.core.Prepare()
	return
}

func (db *standalone) Begin(ctx context.Context, options TransactionOptions) (tx Transaction, err error) {
	core, begErr := db.core.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(options.Isolation),
		ReadOnly:  options.Readonly,
	})
	if begErr != nil {
		err = begErr
		return
	}
	tx = &DefaultTransaction{
		core:       core,
		prepare:    db.prepare,
		statements: db.statements,
	}
	return
}

func (db *standalone) Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error) {
	var r *sql.Rows
	if db.prepare {
		stmt, prepareErr := db.statements.Get(query)
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
		r, err = db.core.QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
		if err != nil {
			return
		}
	}

	rows = &DefaultRows{
		core: r,
	}
	return
}

func (db *standalone) Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error) {
	var r sql.Result
	if db.prepare {
		stmt, prepareErr := db.statements.Get(query)
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
		r, err = db.core.ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
		if err != nil {
			return
		}
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

func (db *standalone) Close(_ context.Context) (err error) {
	if db.prepare {
		db.statements.Close()
	}
	err = db.core.Close()
	return
}
