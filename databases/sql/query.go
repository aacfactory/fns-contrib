package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/mmhash"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/json"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/singleflight"
	"strconv"
)

var (
	queryFnName = []byte("query")
)

func Query(ctx context.Context, query []byte, arguments ...interface{}) (v Rows, err error) {
	tx, hasTx := loadTransaction(ctx)
	if hasTx {
		rows, queryErr := tx.Query(ctx, query, arguments)
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
		Query:     bytex.ToString(query),
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
	db      databases.Database
	group   *transactions.Group
	barrier singleflight.Group
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
			v, err = NewRows(rows)
			if err != nil {
				err = errors.Warning("sql: query failed").WithCause(err)
				return
			}
			return
		}
	}
	buf := bytebufferpool.Get()
	_, _ = buf.Write(query)
	ap, encodeErr := json.Marshal(param.Arguments)
	if encodeErr != nil {
		rows, queryErr := fn.db.Query(r, query, param.Arguments)
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
	_, _ = buf.Write(ap)
	key := strconv.FormatUint(mmhash.Sum64(buf.Bytes()), 16)
	bytebufferpool.Put(buf)
	v, err, _ = fn.barrier.Do(key, func() (v interface{}, err error) {
		rows, queryErr := fn.db.Query(r, query, param.Arguments)
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
	})
	fn.barrier.Forget(key)
	return
}
