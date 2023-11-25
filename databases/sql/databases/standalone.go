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
	Driver      string        `json:"driver"`
	DSN         string        `json:"dsn"`
	MaxIdles    int           `json:"maxIdles"`
	MaxOpens    int           `json:"maxOpens"`
	MaxIdleTime time.Duration `json:"maxIdleTime"`
	MaxLifetime time.Duration `json:"maxLifetime"`
}

type standalone struct {
	log  logs.Logger
	core *sql.DB
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
		core: core,
	}
	return
}

func (db *standalone) Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error) {
	r, queryErr := db.core.QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
	if queryErr != nil {
		err = queryErr
		return
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (db *standalone) Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error) {
	r, execErr := db.core.ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
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

func (db *standalone) Close(_ context.Context) (err error) {
	err = db.core.Close()
	return
}
