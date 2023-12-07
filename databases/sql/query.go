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
	queryFnName = []byte("query")
)

func Query(ctx context.Context, query string, arguments ...interface{}) (v Rows, err error) {
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
		rows, queryErr := tx.Query(ctx, query, arguments)
		if debug && log.DebugEnabled() {
			latency := time.Now().Sub(handleBegin)
			log.Debug().With("succeed", queryErr == nil).With("latency", latency.String()).With("transaction", tx.Id).
				Message(fmt.Sprintf("query debug log:\n- query:\n  %s\n- arguments:\n  %s\n", query, fmt.Sprintf("%+v", arguments)))
		}
		if queryErr != nil {
			err = errors.Warning("sql: query failed").WithCause(queryErr).WithMeta("query", query)
			return
		}
		v, err = NewRows(rows)
		if err != nil {
			err = errors.Warning("sql: query failed").WithCause(err).WithMeta("query", query)
			return
		}
		return
	}
	options := make([]services.RequestOption, 0, 1)
	info, hasInfo, loadInfoErr := loadTransactionInfo(ctx)
	if loadInfoErr != nil {
		err = errors.Warning("sql: query failed").WithCause(loadInfoErr)
		return
	}
	if hasInfo {
		options = append(options, services.WithEndpointId(bytex.FromString(info.EndpointId)))
	}
	eps := runtime.Endpoints(ctx)
	param := queryParam{
		Query:     query,
		Arguments: Arguments(arguments),
	}
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	response, handleErr := eps.Request(ctx, ep, queryFnName, param, options...)
	if handleErr != nil {
		err = handleErr
		return
	}
	v, err = services.ValueOfResponse[Rows](response)
	if err != nil {
		err = errors.Warning("sql: query failed").WithCause(err)
		return
	}
	return
}

type queryParam struct {
	Query     string    `json:"query"`
	Arguments Arguments `json:"arguments"`
}

type queryFn struct {
	debug bool
	log   logs.Logger
	db    databases.Database
	group *transactions.Group
}

func (fn *queryFn) Name() string {
	return string(queryFnName)
}

func (fn *queryFn) Internal() bool {
	return true
}

func (fn *queryFn) Readonly() bool {
	return false
}

func (fn *queryFn) Handle(r services.Request) (v interface{}, err error) {
	param, paramErr := services.ValueOfParam[queryParam](r.Param())
	if paramErr != nil {
		err = errors.Warning("sql: query failed").WithCause(paramErr)
		return
	}
	if len(param.Query) == 0 {
		err = errors.Warning("sql: query failed").WithCause(fmt.Errorf("query is required"))
		return
	}
	info, has, loadErr := loadTransactionInfo(r)
	if loadErr != nil {
		err = errors.Warning("sql: query failed").WithCause(loadErr)
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
			rows, queryErr := tx.Query(r, param.Query, param.Arguments)
			if fn.debug && fn.log.DebugEnabled() {
				latency := time.Now().Sub(handleBegin)
				fn.log.Debug().With("succeed", queryErr == nil).With("latency", latency.String()).With("transaction", info.Id).
					Message(fmt.Sprintf("query debug log:\n- query:\n  %s\n- arguments:\n  %s\n", param.Query, fmt.Sprintf("%+v", param.Arguments)))
			}
			if queryErr != nil {
				err = errors.Warning("sql: query failed").WithCause(queryErr).WithMeta("query", param.Query)
				return
			}
			v, err = NewRows(rows)
			if err != nil {
				err = errors.Warning("sql: query failed").WithCause(err).WithMeta("query", param.Query)
				return
			}
			return
		}
	}
	handleBegin := time.Time{}
	if fn.debug && fn.log.DebugEnabled() {
		useDebugLog(r)
		handleBegin = time.Now()
	}
	rows, queryErr := fn.db.Query(r, param.Query, param.Arguments)
	if fn.debug && fn.log.DebugEnabled() {
		latency := time.Now().Sub(handleBegin)
		fn.log.Debug().With("succeed", err == nil).With("latency", latency.String()).
			Message(fmt.Sprintf("query debug log:\n- query:\n  %s\n- arguments:\n  %s\n", param.Query, fmt.Sprintf("%+v", param.Arguments)))
	}
	if queryErr != nil {
		err = errors.Warning("sql: query failed").WithCause(queryErr)
		return
	}
	v, err = NewRows(rows)
	if err != nil {
		err = errors.Warning("sql: query failed").WithCause(err)
		return
	}
	return
}
