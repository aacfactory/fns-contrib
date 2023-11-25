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
	Driver      string        `json:"driver"`
	Master      string        `json:"master"`
	Slavers     []string      `json:"slavers"`
	MaxIdles    int           `json:"maxIdles"`
	MaxOpens    int           `json:"maxOpens"`
	MaxIdleTime time.Duration `json:"maxIdleTime"`
	MaxLifetime time.Duration `json:"maxLifetime"`
}

type masterSlave struct {
	log        logs.Logger
	master     *sql.DB
	slavers    []*sql.DB
	slaversLen uint32
	pos        uint32
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
		core: core,
	}
	return
}

func (db *masterSlave) Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error) {
	pos := atomic.AddUint32(&db.pos, 1)
	slaver := db.slavers[pos%db.slaversLen]
	r, queryErr := slaver.QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
	if queryErr != nil {
		err = queryErr
		return
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (db *masterSlave) Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error) {
	r, execErr := db.master.ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
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

func (db *masterSlave) Close(_ context.Context) (err error) {
	errs := errors.MakeErrors()
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
