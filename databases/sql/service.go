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

type Service struct {
	running *int64
	client  Client
	gtm     *GlobalTransactionManagement
}

func (svc *Service) Namespace() string {
	return Namespace
}

func (svc *Service) Internal() bool {
	return true
}

func (svc *Service) Build(root configuares.Config) (err error) {
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
	atomic.StoreInt64(svc.running, 1)
	svc.gtm = NewGlobalTransactionManagement()
	return
}

func (svc *Service) Description() (description []byte) {
	return
}

func (svc *Service) Handle(ctx fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	if atomic.LoadInt64(svc.running) == 0 {
		err = errors.New(555, "***WARNING***", "fns SQL Handle: service is not ready or closing")
		return
	}
	switch fn {
	case TxBeginFn:
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
		err = svc.txCommit(ctx)
		result = fns.Empty{}
	case TxRollbackFn:
		err = svc.txRollback(ctx)
		result = fns.Empty{}
	case QueryFn:
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

func (svc *Service) Close() (err error) {
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
