package sql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/internal"
	"github.com/aacfactory/fns/service"
)

const (
	requestLocalTransactionHostId = "_sql_rid"
	dbnameContextKey              = "_sql_dbname"
)

func SwitchDatabase(ctx context.Context, dbname string) context.Context {
	return context.WithValue(ctx, dbnameContextKey, dbname)
}

func currentDatabase(ctx context.Context) (dbname string) {
	dbname0 := ctx.Value(dbnameContextKey)
	if dbname0 == nil {
		dbname = "default"
		return
	}
	dbname = dbname0.(string)
	return
}

func BeginTransaction(ctx context.Context) (err errors.CodeError) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, name, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found")
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Local().Remove(requestLocalTransactionHostId)
		}
		return
	}
	fr := endpoint.Request(ctx, beginTransactionFn, service.NewArgument(&databaseArgument{
		Database: currentDatabase(ctx),
	}))
	r := transactionRegistration{}
	_, getResultErr := fr.Get(ctx, &r)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	if r.Id == "" {
		err = errors.ServiceError("sql: begin transaction failed")
		request.Local().Remove(requestLocalTransactionHostId)
		return
	}
	if rid == "" {
		request.Local().Put(requestLocalTransactionHostId, r.Id)
	}
	return
}

func CommitTransaction(ctx context.Context) (err errors.CodeError) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr)
		return
	}
	if rid == "" {
		err = errors.ServiceError("sql: there is no transaction in context")
		return
	}
	endpoint, hasEndpoint := service.GetExactEndpoint(ctx, name, rid)
	if !hasEndpoint {
		request.Local().Remove(requestLocalTransactionHostId)
		err = errors.NotFound("sql: endpoint was not found").WithMeta("endpointId", rid)
		return
	}
	fr := endpoint.Request(ctx, commitTransactionFn, service.NewArgument(&databaseArgument{
		Database: currentDatabase(ctx),
	}))
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
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr)
		return
	}
	if rid == "" {
		err = errors.ServiceError("sql: there is no transaction in context")
		return
	}
	endpoint, hasEndpoint := service.GetExactEndpoint(ctx, name, rid)
	if !hasEndpoint {
		request.Local().Remove(requestLocalTransactionHostId)
		err = errors.NotFound("sql: endpoint was not found").WithMeta("endpointId", rid)
		return
	}

	fr := endpoint.Request(ctx, rollbackTransactionFn, service.NewArgument(&databaseArgument{
		Database: currentDatabase(ctx),
	}))
	_, getResultErr := fr.Get(ctx, &service.Empty{})
	if getResultErr != nil {
		err = getResultErr
		return
	}
	request.Local().Remove(requestLocalTransactionHostId)
	return
}

func Query(ctx context.Context, query string, args ...interface{}) (v Rows, err errors.CodeError) {
	if query == "" {
		err = errors.BadRequest("sql: invalid query argument")
		return
	}
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, name, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found")
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
		Database: currentDatabase(ctx),
		Query:    query,
		Args:     tuple,
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
	if query == "" {
		err = errors.BadRequest("sql: invalid execute argument")
		return
	}
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := ""
	_, ridErr := request.Local().Scan(requestLocalTransactionHostId, &rid)
	if ridErr != nil {
		err = errors.Warning("sql: can not get transaction host registration id in request context").WithCause(ridErr)
		return
	}
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, name, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: endpoint was not found")
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
		Database: currentDatabase(ctx),
		Query:    query,
		Args:     tuple,
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
