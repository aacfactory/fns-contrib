package sql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/internal"
	"github.com/aacfactory/fns/service"
	"sync"
)

const (
	requestLocalTransactionHostId = "@fns_sql_rid"
	sqlOptionsContextKey          = "@fns_sql_options"
)

var (
	cachedDialect       = new(sync.Map)
	defaultProxyOptions = &ProxyOptions{
		database: name,
	}
)

type ProxyOption func(*ProxyOptions)

type ProxyOptions struct {
	database string
}

func newDefaultProxyOptions() *ProxyOptions {
	return &ProxyOptions{
		database: name,
	}
}

func Database(name string) ProxyOption {
	return func(options *ProxyOptions) {
		options.database = name
	}
}

func WithOptions(ctx context.Context, options ...ProxyOption) context.Context {
	opt := newDefaultProxyOptions()
	if options != nil {
		for _, option := range options {
			option(opt)
		}
	}
	return context.WithValue(ctx, sqlOptionsContextKey, opt)
}

func getOptions(ctx context.Context) (options *ProxyOptions) {
	v := ctx.Value(sqlOptionsContextKey)
	if v == nil {
		options = defaultProxyOptions
		return
	}
	options = v.(*ProxyOptions)
	return
}

func Dialect(ctx context.Context) (dialect string, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	cached, loaded := cachedDialect.Load(database)
	if loaded {
		dialect = cached.(string)
		return
	}
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr).WithMeta("database", database)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, database)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, database, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Local().Remove(requestLocalTransactionHostId)
		}
		return
	}
	fr := endpoint.Request(ctx, databaseDialectFn, service.EmptyArgument())
	r := databaseInfo{}
	_, getResultErr := fr.Get(ctx, &r)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	dialect = r.Dialect
	cachedDialect.Store(database, dialect)
	return
}

func BeginTransaction(ctx context.Context) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr).WithMeta("database", database)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, database)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, database, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Local().Remove(requestLocalTransactionHostId)
		}
		return
	}
	fr := endpoint.Request(ctx, beginTransactionFn, service.EmptyArgument())
	r := transactionRegistration{}
	_, getResultErr := fr.Get(ctx, &r)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	if r.Id == "" {
		err = errors.ServiceError("sql: begin transaction failed").WithMeta("database", database)
		request.Local().Remove(requestLocalTransactionHostId)
		return
	}
	if rid == "" {
		request.Local().Put(requestLocalTransactionHostId, r.Id)
	}
	return
}

func CommitTransaction(ctx context.Context) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr).WithMeta("database", database)
		return
	}
	if rid == "" {
		err = errors.ServiceError("sql: there is no transaction in context")
		return
	}
	endpoint, hasEndpoint := service.GetExactEndpoint(ctx, database, rid)
	if !hasEndpoint {
		request.Local().Remove(requestLocalTransactionHostId)
		err = errors.NotFound("sql: endpoint was not found").WithMeta("endpointId", rid).WithMeta("database", database)
		return
	}
	fr := endpoint.Request(ctx, commitTransactionFn, service.EmptyArgument())
	status := transactionStatus{}
	_, getResultErr := fr.Get(ctx, &status)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	if status.Finished {
		request.Local().Remove(requestLocalTransactionHostId)
	}
	return
}

func RollbackTransaction(ctx context.Context) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr).WithMeta("database", database)
		return
	}
	if rid == "" {
		err = errors.ServiceError("sql: there is no transaction in context").WithMeta("database", database)
		return
	}
	endpoint, hasEndpoint := service.GetExactEndpoint(ctx, database, rid)
	if !hasEndpoint {
		request.Local().Remove(requestLocalTransactionHostId)
		err = errors.NotFound("sql: endpoint was not found").WithMeta("endpointId", rid).WithMeta("database", database)
		return
	}

	fr := endpoint.Request(ctx, rollbackTransactionFn, service.EmptyArgument())
	_, getResultErr := fr.Get(ctx, &service.Empty{})
	if getResultErr != nil {
		err = getResultErr
		return
	}
	request.Local().Remove(requestLocalTransactionHostId)
	return
}

func Query(ctx context.Context, query string, args ...interface{}) (v Rows, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	if query == "" {
		err = errors.BadRequest("sql: invalid query argument").WithMeta("database", database)
		return
	}
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr).WithMeta("database", database)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, database)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, database, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Local().Remove(requestLocalTransactionHostId)
		}
		return
	}
	var tuple *internal.Tuple
	if args != nil && len(args) > 0 {
		tuple = internal.NewTuple().Append(args...)
	}
	fr := endpoint.Request(ctx, queryFn, service.NewArgument(&queryArgument{
		Query: query,
		Args:  tuple,
	}))
	rows0 := &rows{}
	_, getResultErr := fr.Get(ctx, rows0)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	v = rows0
	return
}

func Execute(ctx context.Context, query string, args ...interface{}) (affected int64, lastInsertId int64, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	if query == "" {
		err = errors.BadRequest("sql: invalid execute argument").WithMeta("database", database)
		return
	}
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr).WithMeta("database", database)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, database)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, database, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Local().Remove(requestLocalTransactionHostId)
		}
		return
	}
	var tuple *internal.Tuple
	if args != nil && len(args) > 0 {
		tuple = internal.NewTuple().Append(args...)
	}
	fr := endpoint.Request(ctx, executeFn, service.NewArgument(&executeArgument{
		Query: query,
		Args:  tuple,
	}))
	result := &executeResult{}
	_, getResultErr := fr.Get(ctx, result)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	affected = result.Affected
	lastInsertId = result.LastInsertId
	return
}
