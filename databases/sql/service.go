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
	namespace = "sql"

	txBeginFn    = "tx_begin"
	txCommitFn   = "tx_commit"
	txRollbackFn = "tx_rollback"
	queryFn      = "query"
	executeFn    = "execute"

	daoCacheConfigLoadFn = "dao_cache_config"
)

func Service() fns.Service {
	return &service{}
}

type service struct {
	running        *int64
	enableDebugLog bool
	client         Client
	gtm            *GlobalTransactionManagement
	daoConfig      *DAOConfig
}

func (svc *service) Namespace() string {
	return namespace
}

func (svc *service) Internal() bool {
	return true
}

func (svc *service) Build(root configuares.Config) (err error) {
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
	svc.enableDebugLog = config.EnableDebugLog
	svc.gtm = NewGlobalTransactionManagement()
	svc.daoConfig = &config.DAO
	return
}

func (svc *service) Document() (doc *fns.ServiceDocument) {
	return
}

func (svc *service) Handle(ctx fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	if atomic.LoadInt64(svc.running) == 0 {
		err = errors.New(555, "***WARNING***", "fns SQL Handle: service is not ready or closing")
		return
	}
	switch fn {
	case txBeginFn:
		ctx = fns.WithFn(ctx, fn)
		param := BeginTransactionParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.BadRequest("fns SQL: parse tx begin param failed").WithCause(paramErr)
			return
		}
		err = svc.beginTransaction(ctx, param)
		result = &TxAddress{
			Address: ctx.App().PublicAddress(),
		}
	case txCommitFn:
		ctx = fns.WithFn(ctx, fn)
		err = svc.commitTransaction(ctx)
		result = fns.Empty{}
	case txRollbackFn:
		ctx = fns.WithFn(ctx, fn)
		err = svc.rollbackTransaction(ctx)
		result = fns.Empty{}
	case queryFn:
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
	case executeFn:
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
	case daoCacheConfigLoadFn:
		result = svc.daoConfig
	default:
		err = errors.NotFound(fmt.Sprintf("fns SQL Handle: %s fn was not found", fn))
	}
	return
}

func (svc *service) Shutdown() (err error) {
	atomic.StoreInt64(svc.running, 0)
	svc.gtm.Close()
	err = svc.client.Close()
	return
}

func (svc *service) getExecutor(ctx fns.Context) (v Executor) {
	tx, hasTx := svc.getTransaction(ctx)
	if hasTx {
		v = tx
	} else {
		v = svc.client.Writer()
	}
	return
}

func (svc *service) getQueryAble(ctx fns.Context) (v QueryAble) {
	tx, hasTx := svc.getTransaction(ctx)
	if hasTx {
		v = tx
	} else {
		v = svc.client.Reader()
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

type Param struct {
	Query string `json:"query,omitempty"`
	Args  *Tuple `json:"args,omitempty"`
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
