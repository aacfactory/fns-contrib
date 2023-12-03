package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	fLog "github.com/aacfactory/fns/logs"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/logs"
	"time"
)

var (
	executeFnName = []byte("execute")
)

func Execute(ctx context.Context, query []byte, arguments ...interface{}) (result databases.Result, err error) {
	tx, hasTx := loadTransaction(ctx)
	if hasTx {
		var log logs.Logger
		debug := debugLogEnabled(ctx)
		handleBegin := time.Time{}
		if debug {
			log = fLog.Load(ctx)
			if log.DebugEnabled() {
				handleBegin = time.Now()
			}
		}
		result, err = tx.Execute(ctx, query, arguments)
		if debug && log.DebugEnabled() {
			latency := time.Now().Sub(handleBegin)
			log.Debug().With("succeed", err == nil).With("latency", latency.String()).With("transaction", tx.Id).
				Message(fmt.Sprintf("execute debug log:\n- query:\n  %s\n- arguments:\n  %s\n", bytex.ToString(query), fmt.Sprintf("%+v", arguments)))
		}
		if err != nil {
			err = errors.Warning("sql: execute failed").WithCause(err)
			return
		}
		return
	}
	options := make([]services.RequestOption, 0, 1)
	info, hasInfo, loadInfoErr := loadTransactionInfo(ctx)
	if loadInfoErr != nil {
		err = errors.Warning("sql: execute failed").WithCause(loadInfoErr)
		return
	}
	if hasInfo {
		options = append(options, services.WithEndpointId(bytex.FromString(info.EndpointId)))
	}
	eps := runtime.Endpoints(ctx)
	param := executeParam{
		Query:     bytex.ToString(query),
		Arguments: Arguments(arguments),
	}
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	response, handleErr := eps.Request(ctx, ep, executeFnName, param, options...)
	if handleErr != nil {
		err = handleErr
		return
	}
	result, err = services.ValueOfResponse[databases.Result](response)
	if err != nil {
		err = errors.Warning("sql: execute failed").WithCause(err)
		return
	}
	return
}

type executeParam struct {
	Query     string    `json:"query"`
	Arguments Arguments `json:"arguments"`
}

type executeFn struct {
	debug bool
	log   logs.Logger
	db    databases.Database
	group *transactions.Group
}

func (fn *executeFn) Name() string {
	return string(executeFnName)
}

func (fn *executeFn) Internal() bool {
	return true
}

func (fn *executeFn) Readonly() bool {
	return false
}

func (fn *executeFn) Handle(r services.Request) (v interface{}, err error) {
	param, paramErr := services.ValueOfParam[executeParam](r.Param())
	if paramErr != nil {
		err = errors.Warning("sql: execute failed").WithCause(paramErr)
		return
	}
	if len(param.Query) == 0 {
		err = errors.Warning("sql: execute failed").WithCause(fmt.Errorf("query is required"))
		return
	}
	query := bytex.FromString(param.Query)
	info, has, loadErr := loadTransactionInfo(r)
	if loadErr != nil {
		err = errors.Warning("sql: execute failed").WithCause(loadErr)
		return
	}
	if has {
		tx, hasTx := fn.group.Get(bytex.FromString(info.Id))
		if hasTx && !tx.Closed() {
			handleBegin := time.Time{}
			if fn.debug && fn.log.DebugEnabled() {
				useDebugLog(r)
				handleBegin = time.Now()
			}
			result, executeErr := tx.Execute(r, query, param.Arguments)
			if fn.debug && fn.log.DebugEnabled() {
				latency := time.Now().Sub(handleBegin)
				fn.log.Debug().With("succeed", executeErr == nil).With("latency", latency.String()).With("transaction", info.Id).
					Message(fmt.Sprintf("execute debug log:\n- query:\n  %s\n- arguments:\n  %s\n", bytex.ToString(query), fmt.Sprintf("%+v", param.Arguments)))
			}
			if executeErr != nil {
				err = errors.Warning("sql: execute failed").WithCause(executeErr)
				return
			}
			v = result
			return
		}
	}
	handleBegin := time.Time{}
	if fn.debug && fn.log.DebugEnabled() {
		useDebugLog(r)
		handleBegin = time.Now()
	}
	result, executeErr := fn.db.Execute(r, query, param.Arguments)
	if fn.debug && fn.log.DebugEnabled() {
		latency := time.Now().Sub(handleBegin)
		fn.log.Debug().With("succeed", executeErr == nil).With("latency", latency.String()).
			Message(fmt.Sprintf("execute debug log:\n- query:\n  %s\n- arguments:\n  %s\n", bytex.ToString(query), fmt.Sprintf("%+v", param.Arguments)))
	}
	if executeErr != nil {
		err = errors.Warning("sql: execute failed").WithCause(executeErr)
		return
	}
	v = result
	return
}
