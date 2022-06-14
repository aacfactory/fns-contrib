package sql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

const (
	requestHeaderTransactionHostId = "X-Transaction-Rid"
)

func BeginTransaction(ctx context.Context) (err errors.CodeError) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	rid := request.Header().Get(requestHeaderTransactionHostId)
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, name, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: sql endpoint was not found")
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Header().Raw().Del(requestHeaderTransactionHostId)
		}
		return
	}
	fr := endpoint.Request(ctx, beginTransactionFn, service.NewArgument(nil))
	r := transactionRegistration{}
	_, getResultErr := fr.Get(ctx, &r)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	if r.Id == "" {
		err = errors.ServiceError("sql: begin transaction failed")
		request.Header().Raw().Del(requestHeaderTransactionHostId)
		return
	}
	if rid == "" {
		request.Header().Raw().Set(requestHeaderTransactionHostId, r.Id)
	}
	return
}

func CommitTransaction(ctx context.Context) (err errors.CodeError) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	rid := request.Header().Get(requestHeaderTransactionHostId)
	if rid == "" {
		err = errors.ServiceError("sql: there is no transaction in context")
		return
	}
	endpoint, hasEndpoint := service.GetExactEndpoint(ctx, name, rid)
	if !hasEndpoint {
		request.Header().Raw().Del(requestHeaderTransactionHostId)
		err = errors.NotFound("sql: sql endpoint was not found").WithMeta("endpointId", rid)
		return
	}
	fr := endpoint.Request(ctx, commitTransactionFn, service.NewArgument(nil))
	status := transactionStatus{}
	_, getResultErr := fr.Get(ctx, &status)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	if status.Finished {
		request.Header().Raw().Del(requestHeaderTransactionHostId)
	}
	return
}

func RollbackTransaction(ctx context.Context) (err errors.CodeError) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: can not get request in context")
		return
	}
	rid := request.Header().Get(requestHeaderTransactionHostId)
	if rid == "" {
		err = errors.ServiceError("sql: there is no transaction in context")
		return
	}
	endpoint, hasEndpoint := service.GetExactEndpoint(ctx, name, rid)
	if !hasEndpoint {
		request.Header().Raw().Del(requestHeaderTransactionHostId)
		err = errors.NotFound("sql: sql endpoint was not found").WithMeta("endpointId", rid)
		return
	}
	fr := endpoint.Request(ctx, rollbackTransactionFn, service.NewArgument(nil))
	_, getResultErr := fr.Get(ctx, &service.Empty{})
	if getResultErr != nil {
		err = getResultErr
		return
	}
	request.Header().Raw().Del(requestHeaderTransactionHostId)
	return
}

func Query(ctx context.Context, query string, args *Tuple) (rows *Rows, err errors.CodeError) {
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
	rid := request.Header().Get(requestHeaderTransactionHostId)
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, name, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: sql endpoint was not found")
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Header().Raw().Del(requestHeaderTransactionHostId)
		}
		return
	}
	fr := endpoint.Request(ctx, queryFn, service.NewArgument(&queryArgument{
		Query: query,
		Args:  args,
	}))
	rows = &Rows{}
	_, getResultErr := fr.Get(ctx, rows)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	return
}

func Execute(ctx context.Context, query string, args *Tuple) (result *ExecuteResult, err errors.CodeError) {
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
	rid := request.Header().Get(requestHeaderTransactionHostId)
	if rid == "" {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	} else {
		endpoint, hasEndpoint = service.GetExactEndpoint(ctx, name, rid)
	}
	if !hasEndpoint {
		err = errors.NotFound("sql: sql endpoint was not found")
		if rid != "" {
			err = err.WithMeta("endpointId", rid)
			request.Header().Raw().Del(requestHeaderTransactionHostId)
		}
		return
	}
	fr := endpoint.Request(ctx, executeFn, service.NewArgument(&executeArgument{
		Query: query,
		Args:  args,
	}))
	result = &ExecuteResult{}
	_, getResultErr := fr.Get(ctx, result)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	return
}
