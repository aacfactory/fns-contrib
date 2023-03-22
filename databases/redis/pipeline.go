package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
)

const (
	pipelineFn = "pipeline"
	execFn     = "exec"
	discardFn  = "discard"
)

type PipelineParam struct {
	Tx bool
}

type pipelineResult struct {
	Id string
}

func Pipeline(ctx context.Context, tx bool) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	param, paramErr := newProxyParam(database, PipelineParam{
		Tx: tx,
	})
	if paramErr != nil {
		err = errors.Map(paramErr)
		return
	}

	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("redis: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	pid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(pid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}
	if !hasEndpoint {
		err = errors.NotFound("redis: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(pid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
		}
		return
	}

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, pipelineFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		err = errors.Warning("redis: pipeline of database was not declared").WithMeta("database", database)
		return
	}
	r := pipelineResult{}
	scanErr := result.Scan(&r)
	if scanErr != nil {
		err = scanErr
		return
	}
	if r.Id == "" {
		err = errors.ServiceError("redis: begin pipeline failed").WithMeta("database", database)
		request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
		return
	}
	if !hasRid {
		request.Trunk().Put(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database), bytex.FromString(r.Id))
	}
	return
}

type ExecResult struct {
	Finished bool
	Cmders   []ExecResultCmder
}

type ExecResultCmder struct {
	Name  string
	Error string
}

func Exec(ctx context.Context) (v []ExecResultCmder, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	param, paramErr := newProxyParam(database, nil)
	if paramErr != nil {
		err = errors.Map(paramErr)
		return
	}

	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("redis: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	pid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(pid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}
	if !hasEndpoint {
		err = errors.NotFound("redis: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(pid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
		}
		return
	}

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, execFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	r := ExecResult{}
	scanErr := result.Scan(&r)
	if scanErr != nil {
		err = scanErr
		return
	}
	if r.Finished {
		request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
	}
	v = r.Cmders
	return
}

func Discard(ctx context.Context) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	param, paramErr := newProxyParam(database, nil)
	if paramErr != nil {
		err = errors.Map(paramErr)
		return
	}

	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = errors.Warning("redis: can not get request in context").WithMeta("database", database)
		return
	}
	var endpoint service.Endpoint
	hasEndpoint := false
	pid, hasRid := request.Trunk().Get(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
	if hasRid {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name, service.Exact(bytex.ToString(pid)))
	} else {
		endpoint, hasEndpoint = service.GetEndpoint(ctx, name)
	}
	if !hasEndpoint {
		err = errors.NotFound("redis: endpoint was not found").WithMeta("database", database)
		if hasRid {
			err = err.WithMeta("endpointId", bytex.ToString(pid))
			request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
		}
		return
	}

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, discardFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}
	request.Trunk().Remove(fmt.Sprintf("%s:%s", requestLocalPipelineHostId, database))
	return
}
