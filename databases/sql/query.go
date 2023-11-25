package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	queryFnName = []byte("query")
)

func Query(ctx context.Context, query string, arguments ...interface{}) (rows databases.Rows, err error) {
	tx, hasTx, loadTxErr := loadTransaction(ctx)
	if loadTxErr != nil {
		err = errors.Warning("sql: query failed").WithCause(loadTxErr)
		return
	}
	if hasTx {
		rows, err = tx.Query(ctx, bytex.FromString(query), arguments)
		if err != nil {
			err = errors.Warning("sql: query failed").WithCause(err)
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
	response, handleErr := eps.Request(ctx, endpointName, queryFnName, param, options...)
	if handleErr != nil {
		err = handleErr
		return
	}
	rows, err = services.ValueOfResponse[databases.Rows](response)
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
	query := bytex.FromString(param.Query)
	info, has, loadErr := loadTransactionInfo(r)
	if loadErr != nil {
		err = errors.Warning("sql: query failed").WithCause(loadErr)
		return
	}
	if has {
		tx, hasTx := fn.group.Get(bytex.FromString(info.Id))
		if hasTx && !tx.Closed() {
			rows, queryErr := tx.Query(r, query, param.Arguments)
			if queryErr != nil {
				err = errors.Warning("sql: query failed").WithCause(queryErr)
				return
			}
			v = rows
			return
		}
	}
	rows, queryErr := fn.db.Query(r, query, param.Arguments)
	if queryErr != nil {
		err = errors.Warning("sql: query failed").WithCause(queryErr)
		return
	}
	v = rows
	return
}
