package sql

import (
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"sync/atomic"
)

const (
	configPath = "sql"
)

const (
	Namespace = "sql"

	TxBeginFn    = "tx_begin"
	TxCommitFn   = "tx_commit"
	TxRollbackFn = "tx_rollback"
	QueryFn      = "query"
	ExecuteFn    = "execute"
)

func Service() fns.Service {
	return &_service{}
}

type _service struct {
	running *int64
	client  Client
	gtm     *GlobalTransactionManagement
}

func (svc *_service) Namespace() string {
	return Namespace
}

func (svc *_service) Internal() bool {
	return true
}

func (svc *_service) Build(root configuares.Config) (err error) {
	config := Config{}
	has, readErr := root.Get(configPath, &config)
	if readErr != nil {
		err = fmt.Errorf("fns SQL Build: read config failed, %v", readErr)
		return
	}
	if !has {
		err = fmt.Errorf("fns SQL Build: no sql path in root config")
		return
	}

	client, createErr := config.CreateClient()
	if createErr != nil {
		err = createErr
		return
	}

	svc.client = client
	running := int64(0)
	svc.running = &running
	atomic.StoreInt64(svc.running, 1)
	svc.gtm = NewGlobalTransactionManagement()
	return
}

func (svc *_service) Description() (description []byte) {
	return
}

func (svc *_service) Handle(ctx fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	if atomic.LoadInt64(svc.running) == 0 {
		err = errors.New(555, "***WARNING***", "fns SQL Handle: service is not ready or closing")
		return
	}
	switch fn {
	case TxBeginFn:
		ctx = fns.WithFn(ctx, fn)
		param := TxBeginParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.BadRequest("fns SQL: parse tx begin param failed").WithCause(paramErr)
			return
		}
		err = svc.txBegin(ctx, param)
		result = &TxAddress{
			Address: ctx.App().PublicAddress(),
		}
	case TxCommitFn:
		ctx = fns.WithFn(ctx, fn)
		err = svc.txCommit(ctx)
		result = fns.Empty{}
	case TxRollbackFn:
		ctx = fns.WithFn(ctx, fn)
		err = svc.txRollback(ctx)
		result = fns.Empty{}
	case QueryFn:
		ctx = fns.WithFn(ctx, fn)
		param := Param{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.BadRequest("fns SQL: parse query fn param failed").WithCause(paramErr)
			return
		}
		rows, queryErr := svc.queryFn(ctx, param)
		if queryErr != nil {
			err = queryErr
			return
		}
		result = rows
	case ExecuteFn:
		ctx = fns.WithFn(ctx, fn)
		param := Param{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.BadRequest("fns SQL: parse execute fn param failed").WithCause(paramErr)
			return
		}
		execResult, execErr := svc.executeFn(ctx, param)
		if execErr != nil {
			err = execErr
			return
		}
		result = execResult
	default:
		err = errors.NotFound(fmt.Sprintf("fns SQL Handle: %s fn was not found", fn))
	}
	return
}

func (svc *_service) Close() (err error) {
	atomic.StoreInt64(svc.running, 0)
	svc.gtm.Close()
	err = svc.client.Close()
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

type Param struct {
	Query string `json:"query,omitempty"`
	Args  *Tuple `json:"args,omitempty"`
	InTx  bool   `json:"inTx,omitempty"`
}

// +-------------------------------------------------------------------------------------------------------------------+

type ExecResult struct {
	Affected     int64 `json:"affected,omitempty"`
	LastInsertId int64 `json:"lastInsertId,omitempty"`
}

// +-------------------------------------------------------------------------------------------------------------------+

type TxAddress struct {
	Address string `json:"address,omitempty"`
}
