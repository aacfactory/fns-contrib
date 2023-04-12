package sql

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
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
		database: "",
	}
)

type ProxyOption func(*ProxyOptions)

type ProxyOptions struct {
	database string
}

func newProxyOptions() *ProxyOptions {
	return &ProxyOptions{
		database: "",
	}
}

func Database(name string) ProxyOption {
	return func(options *ProxyOptions) {
		options.database = name
	}
}

func WithOptions(ctx context.Context, options ...ProxyOption) context.Context {
	opt := newProxyOptions()
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

type dialectArgument struct {
	Database string `json:"database"`
}

type dialectResult struct {
	Dialect string `json:"dialect"`
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
	rid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(rid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(rid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		}
		return
	}
	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, databaseDialectFn, service.NewArgument(&dialectArgument{
		Database: database,
	})))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		err = errors.Warning("sql: dialect of database was not declared").WithMeta("database", database)
		return
	}
	r := dialectResult{}
	scanErr := result.Scan(&r)
	if scanErr != nil {
		err = scanErr
		return
	}
	dialect = r.Dialect
	cachedDialect.Store(database, dialect)
	return
}

type transactionBeginArgument struct {
	Database string `json:"database"`
}

type transactionRegistration struct {
	Id string `json:"id"`
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
	rid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(rid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(rid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		}
		return
	}

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, beginTransactionFn, service.NewArgument(&transactionBeginArgument{
		Database: database,
	})))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		err = errors.Warning("sql: transaction of database was not declared").WithMeta("database", database)
		return
	}
	r := transactionRegistration{}
	scanErr := result.Scan(&r)
	if scanErr != nil {
		err = scanErr
		return
	}
	if r.Id == "" {
		err = errors.ServiceError("sql: begin transaction failed").WithMeta("database", database)
		request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		return
	}
	if !hasRid {
		request.Trunk().Put(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database), bytex.FromString(r.Id))
	}
	return
}

type transactionCommitArgument struct {
	Database string `json:"database"`
}

type transactionStatus struct {
	Finished bool `json:"finished"`
}

func CommitTransaction(ctx context.Context) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}

	rid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	if !hasRid {
		err = errors.ServiceError("sql: there is no transaction in context")
		return
	}

	endpoint, hasEndpoint := service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(rid)))
	if !hasEndpoint {
		request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		err = errors.NotFound("sql: endpoint was not found").WithMeta("endpointId", bytex.ToString(rid)).WithMeta("database", database)
		return
	}
	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, commitTransactionFn, service.NewArgument(&transactionCommitArgument{
		Database: database,
	})))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		err = errors.Warning("sql: transaction of database was not declared").WithMeta("database", database)
		return
	}
	status := transactionStatus{}
	scanErr := result.Scan(&status)
	if scanErr != nil {
		err = scanErr
		return
	}
	if status.Finished {
		request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	}
	return
}

type transactionRollbackArgument struct {
	Database string `json:"database"`
}

func RollbackTransaction(ctx context.Context) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context").WithMeta("database", database)
		return
	}

	rid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	if !hasRid {
		err = errors.ServiceError("sql: there is no transaction in context").WithMeta("database", database)
		return
	}

	endpoint, hasEndpoint := service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(rid)))
	if !hasEndpoint {
		request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		err = errors.NotFound("sql: endpoint was not found").WithMeta("endpointId", bytex.ToString(rid)).WithMeta("database", database)
		return
	}

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, rollbackTransactionFn, service.NewArgument(&transactionRollbackArgument{
		Database: database,
	})))
	if requestErr != nil {
		err = requestErr
		return
	}
	request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	return
}

type queryArgument struct {
	Database string     `json:"database"`
	Query    string     `json:"query"`
	Args     *Arguments `json:"args"`
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

	rid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(rid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}

	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(rid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		}
		return
	}
	var tuple *Arguments
	if args != nil && len(args) > 0 {
		tuple = NewArguments().Append(args...)
	}
	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, queryFn, service.NewArgument(&queryArgument{
		Database: database,
		Query:    query,
		Args:     tuple,
	})))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		err = errors.Warning("sql: rows of query result was not declared").WithMeta("database", database)
		return
	}
	rows0 := rows{}
	scanErr := result.Scan(&rows0)
	if scanErr != nil {
		err = scanErr
		return
	}
	v = &rows0
	return
}

type executeArgument struct {
	Database string     `json:"database"`
	Query    string     `json:"query"`
	Args     *Arguments `json:"args"`
}

type executeResult struct {
	Affected     int64 `json:"affected"`
	LastInsertId int64 `json:"lastInsertId"`
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

	rid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(rid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}

	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(rid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalTransactionHostId, database))
		}
		return
	}
	var tuple *Arguments
	if args != nil && len(args) > 0 {
		tuple = NewArguments().Append(args...)
	}

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, executeFn, service.NewArgument(&queryArgument{
		Database: database,
		Query:    query,
		Args:     tuple,
	})))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		err = errors.Warning("sql: result of execute result was not declared").WithMeta("database", database)
		return
	}
	r := executeResult{}
	scanErr := result.Scan(&r)
	if scanErr != nil {
		err = scanErr
		return
	}

	affected = r.Affected
	lastInsertId = r.LastInsertId
	return
}
