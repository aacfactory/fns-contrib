package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"github.com/cespare/xxhash/v2"
	"github.com/valyala/bytebufferpool"
	"strings"
	"sync/atomic"
	"time"
)

type Database interface {
	BeginTransaction(ctx context.Context) (err errors.CodeError)
	CommitTransaction(ctx context.Context) (finished bool, err errors.CodeError)
	RollbackTransaction(ctx context.Context) (err errors.CodeError)
	Query(ctx context.Context, query string, args []interface{}) (rows *sql.Rows, err errors.CodeError)
	Execute(ctx context.Context, query string, args []interface{}) (result sql.Result, err errors.CodeError)
	Close()
}

type Config map[string]DatabaseConfig

type DatabaseConfig struct {
	Driver           string   `json:"driver"`
	MasterSlaverMode bool     `json:"masterSlaverMode"`
	DSN              []string `json:"dsn"`
	MaxIdles         int      `json:"maxIdles"`
	MaxOpens         int      `json:"maxOpens"`
	EnableDebugLog   bool     `json:"enableDebugLog"`
	GTMCleanUpSecond int      `json:"gtmCleanUpSecond"`
	Isolation        int      `json:"isolation"`
}

type Options struct {
	Log     logs.Logger
	Config  Config
	Barrier service.Barrier
}

func New(options Options) (v map[string]Database, err error) {
	v = make(map[string]Database)
	hasDefault := false
	for name, config := range options.Config {
		name = strings.TrimSpace(name)
		if !hasDefault {
			hasDefault = strings.ToLower(name) == "default"
		}
		client, clientErr := newClient(config)
		if clientErr != nil {
			err = clientErr
			return
		}
		isolation := sql.IsolationLevel(config.Isolation)
		if isolation < sql.LevelDefault || isolation > sql.LevelLinearizable {
			isolation = sql.LevelReadCommitted
		}
		v[name] = &db{
			running:           1,
			log:               options.Log.With("sql", "db"),
			enableSQLDebugLog: config.EnableDebugLog,
			isolation:         isolation,
			client:            client,
			gtm: newGlobalTransactionManagement(globalTransactionManagementOptions{
				log:             options.Log,
				checkupInterval: time.Duration(config.GTMCleanUpSecond) * time.Second,
			}),
			barrier: options.Barrier,
		}
	}
	return
}

type db struct {
	running           int64
	log               logs.Logger
	enableSQLDebugLog bool
	isolation         sql.IsolationLevel
	client            Client
	gtm               *globalTransactionManagement
	barrier           service.Barrier
}

func (db *db) Close() {
	atomic.StoreInt64(&db.running, 0)
	closeErr := db.client.Close()
	if closeErr != nil {
		if db.log.DebugEnabled() {
			db.log.Debug().Caller().Cause(closeErr).Message("db: close failed")
		}
	}
	db.gtm.Close()
}

func (db *db) isNotRunning() bool {
	return atomic.LoadInt64(&db.running) != 1
}

func (db *db) BeginTransaction(ctx context.Context) (err errors.CodeError) {
	if db.isNotRunning() {
		err = errors.Unavailable("sql: service is closed")
		return
	}
	beginErr := db.gtm.Begin(ctx, db.client.Writer(), db.isolation)
	if beginErr != nil {
		err = errors.ServiceError("sql: begin transaction failed").WithCause(beginErr)
		return
	}
	return
}

func (db *db) CommitTransaction(ctx context.Context) (finished bool, err errors.CodeError) {
	if db.isNotRunning() {
		err = errors.Unavailable("sql: service is closed")
		return
	}
	ok, commitErr := db.gtm.Commit(ctx)
	if commitErr != nil {
		err = errors.ServiceError("sql: commit transaction failed").WithCause(commitErr)
		return
	}
	finished = ok
	return
}

func (db *db) RollbackTransaction(ctx context.Context) (err errors.CodeError) {
	if db.isNotRunning() {
		err = errors.Unavailable("sql: service is closed")
		return
	}
	db.gtm.Rollback(ctx)
	return
}

func (db *db) Query(ctx context.Context, query string, args []interface{}) (rows *sql.Rows, err errors.CodeError) {
	if db.isNotRunning() {
		err = errors.Unavailable("sql: service is closed")
		return
	}
	begin := time.Time{}
	if db.enableSQLDebugLog && db.log.DebugEnabled() {
		begin = time.Now()
	}
	tx, hasTx := db.gtm.Get(ctx)
	if hasTx {
		rows, err = db.queryWithTransaction(ctx, tx, query, args)
	} else {
		reader := db.client.Reader()
		buf := bytebufferpool.Get()
		_, _ = buf.WriteString(query)
		if args != nil && len(args) > 0 {
			for _, arg := range args {
				_, _ = buf.WriteString(fmt.Sprintf("%v", arg))
			}
		}
		key := fmt.Sprintf("sql:query:%d", xxhash.Sum64(buf.Bytes()))
		bytebufferpool.Put(buf)
		result, doErr, _ := db.barrier.Do(ctx, key, func() (result interface{}, err errors.CodeError) {
			var queryResult *sql.Rows
			var queryErr error
			if args == nil || len(args) == 0 {
				queryResult, queryErr = reader.QueryContext(ctx, query)
			} else {
				queryResult, queryErr = reader.QueryContext(ctx, query, args...)
			}
			if queryErr != nil {
				err = errors.ServiceError("sql: query failed").WithCause(queryErr)
				return
			}
			result = queryResult
			return
		})
		if doErr != nil {
			err = doErr
		} else {
			rows = result.(*sql.Rows)
		}
		db.barrier.Forget(ctx, key)
	}
	if db.enableSQLDebugLog && db.log.DebugEnabled() {
		db.log.Debug().Caller().With("succeed", err != nil).With("latency", time.Now().Sub(begin)).Message(fmt.Sprintf("\n%s\n", query))
	}
	return
}

func (db *db) queryWithTransaction(ctx context.Context, tx *sql.Tx, query string, args []interface{}) (rows *sql.Rows, err errors.CodeError) {
	var queryErr error
	if args == nil || len(args) == 0 {
		rows, queryErr = tx.QueryContext(ctx, query)
	} else {
		rows, queryErr = tx.QueryContext(ctx, query, args...)
	}
	if queryErr != nil {
		db.gtm.Rollback(ctx)
		err = errors.ServiceError("sql: query failed").WithCause(queryErr)
		return
	}
	return
}

func (db *db) Execute(ctx context.Context, query string, args []interface{}) (result sql.Result, err errors.CodeError) {
	if db.isNotRunning() {
		err = errors.Unavailable("sql: service is closed")
		return
	}
	begin := time.Time{}
	if db.enableSQLDebugLog && db.log.DebugEnabled() {
		begin = time.Now()
	}
	var writer Executor = nil
	tx, hasTx := db.gtm.Get(ctx)
	if hasTx {
		writer = tx
	} else {
		writer = db.client.Writer()
	}
	var executeErr error
	if args == nil || len(args) == 0 {
		result, executeErr = writer.ExecContext(ctx, query)
	} else {
		result, executeErr = writer.ExecContext(ctx, query, args...)
	}
	if db.enableSQLDebugLog && db.log.DebugEnabled() {
		db.log.Debug().Caller().With("succeed", executeErr != nil).With("latency", time.Now().Sub(begin)).Message(fmt.Sprintf("\n%s\n", query))
	}
	if executeErr != nil {
		if hasTx {
			db.gtm.Rollback(ctx)
		}
		err = errors.ServiceError("sql: execute failed").WithCause(executeErr)
		return
	}
	return
}
