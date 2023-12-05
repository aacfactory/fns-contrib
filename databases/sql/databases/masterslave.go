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

func MasterSlave() Database {
	return &masterSlave{}
}

type masterSlaveConfig struct {
	Driver      string           `json:"driver"`
	Master      string           `json:"master"`
	Slavers     []string         `json:"slavers"`
	MaxIdles    int              `json:"maxIdles"`
	MaxOpens    int              `json:"maxOpens"`
	MaxIdleTime time.Duration    `json:"maxIdleTime"`
	MaxLifetime time.Duration    `json:"maxLifetime"`
	Statements  StatementsConfig `json:"statements"`
}

type masterSlave struct {
	log               logs.Logger
	master            *sql.DB
	slavers           []*sql.DB
	slaversLen        uint32
	pos               uint32
	prepare           bool
	masterStatements  *Statements
	slaversStatements []*Statements
}

func (db *masterSlave) Name() string {
	return "masterSlave"
}

func (db *masterSlave) Construct(options Options) (err error) {
	db.log = options.Log
	config := masterSlaveConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("sql: master-slave database construct failed").WithCause(configErr)
		return
	}
	// master
	master, masterErr := sql.Open(config.Driver, config.Master)
	if masterErr != nil {
		err = errors.Warning("sql: master-slave database construct failed").WithCause(masterErr)
		return
	}
	maxIdles := config.MaxIdles
	if maxIdles > 0 {
		master.SetMaxIdleConns(maxIdles)
	}
	maxOpens := config.MaxOpens
	if maxOpens > 0 {
		master.SetMaxOpenConns(maxOpens)
	}
	maxIdleTime := config.MaxIdleTime
	if maxIdleTime > 0 {
		master.SetConnMaxIdleTime(maxIdleTime)
	}
	maxLifetime := config.MaxLifetime
	if maxLifetime > 0 {
		master.SetConnMaxLifetime(maxLifetime)
	}
	err = master.Ping()
	if err != nil {
		err = errors.Warning("sql: master-slave database construct failed").WithCause(err)
		return
	}
	db.master = master
	// slaver
	db.slavers = make([]*sql.DB, 0, len(config.Slavers))
	for _, slaverDSN := range config.Slavers {
		slaver, slaverErr := sql.Open(config.Driver, slaverDSN)
		if slaverErr != nil {
			err = errors.Warning("sql: master-slave database construct failed").WithCause(slaverErr)
			return
		}
		if maxIdles > 0 {
			slaver.SetMaxIdleConns(maxIdles)
		}
		if maxOpens > 0 {
			slaver.SetMaxOpenConns(maxOpens)
		}
		if maxIdleTime > 0 {
			slaver.SetConnMaxIdleTime(maxIdleTime)
		}
		if maxLifetime > 0 {
			slaver.SetConnMaxLifetime(maxLifetime)
		}
		err = slaver.Ping()
		if err != nil {
			err = errors.Warning("sql: master-slave database construct failed").WithCause(err)
			return
		}
		db.slavers = append(db.slavers, slaver)
	}
	db.slaversLen = uint32(len(db.slavers))
	if db.slaversLen == 0 {
		err = errors.Warning("sql: master-slave database construct failed").WithCause(fmt.Errorf("no slavers"))
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
		db.masterStatements, err = NewStatements(db.log, db.master, cacheSize, time.Duration(evictTimeoutSeconds)*time.Second)
		if err != nil {
			err = errors.Warning("sql: master-slave database construct failed").WithCause(err)
			return
		}
		for _, slaver := range db.slavers {
			slaverStatements, stmtsErr := NewStatements(db.log, slaver, cacheSize, time.Duration(evictTimeoutSeconds)*time.Second)
			if stmtsErr != nil {
				err = errors.Warning("sql: master-slave database construct failed").WithCause(stmtsErr)
				return
			}
			db.slaversStatements = append(db.slaversStatements, slaverStatements)
		}
		db.prepare = true
	}
	return
}

func (db *masterSlave) Begin(ctx context.Context, options TransactionOptions) (tx Transaction, err error) {
	core, begErr := db.master.BeginTx(ctx, &sql.TxOptions{
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
		statements: db.masterStatements,
	}
	return
}

func (db *masterSlave) Query(ctx context.Context, query []byte, args []any) (rows Rows, err error) {
	pos := atomic.AddUint32(&db.pos, 1) % db.slaversLen
	var r *sql.Rows
	if db.prepare {
		stmts := db.slaversStatements[pos]
		stmt, prepareErr := stmts.Get(query)
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
		slaver := db.slavers[pos]
		r, err = slaver.QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
		if err != nil {
			return
		}
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (db *masterSlave) Execute(ctx context.Context, query []byte, args []any) (result Result, err error) {
	var r sql.Result
	if db.prepare {
		stmt, prepareErr := db.masterStatements.Get(query)
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
		r, err = db.master.ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
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

func (db *masterSlave) Close(_ context.Context) (err error) {
	errs := errors.MakeErrors()
	if db.prepare {
		db.masterStatements.Close()
		for _, statement := range db.slaversStatements {
			statement.Close()
		}
	}

	if closeErr := db.master.Close(); closeErr != nil {
		errs.Append(closeErr)
	}
	for _, slaver := range db.slavers {
		if closeErr := slaver.Close(); closeErr != nil {
			errs.Append(closeErr)
		}
	}
	if len(errs) > 0 {
		err = errors.Warning("sql: master-slave database close failed").WithCause(errs.Error())
		return
	}
	return
}
