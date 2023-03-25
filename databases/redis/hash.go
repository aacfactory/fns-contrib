package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	rds "github.com/redis/go-redis/v9"
)

const (
	hGetALLFn = "h_get_all"
)

func HGetALL(ctx context.Context, param string) (values map[string]string, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, param)
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, hGetALLFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	values = make(map[string]string)
	scanErr := result.Scan(&values)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func hGetALL(ctx context.Context, cmder rds.Cmdable, key string) (values map[string]string, err error) {
	values, err = cmder.HGetAll(ctx, key).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: hgetall failed").WithCause(err)
		return
	}
	return
}

const (
	hDelFn = "h_del"
)

type HDelParam struct {
	Key    string
	fields []string
}

func HDel(ctx context.Context, param HDelParam) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, param)
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, hDelFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func hDel(ctx context.Context, cmder rds.Cmdable, param HDelParam) (n int64, err error) {
	n, err = cmder.HDel(ctx, param.Key, param.fields...).Result()
	if err != nil {
		err = errors.Warning("redis: hdel failed").WithCause(err)
		return
	}
	return
}

const (
	hExistFn = "h_exist"
)

type HExistParam struct {
	Key   string
	field string
}

func HExist(ctx context.Context, param HExistParam) (ok bool, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, param)
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, hExistFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	scanErr := result.Scan(&ok)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func hExist(ctx context.Context, cmder rds.Cmdable, param HExistParam) (ok bool, err error) {
	ok, err = cmder.HExists(ctx, param.Key, param.field).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: hexist failed").WithCause(err)
		return
	}
	return
}

const (
	hGetFn = "h_get"
)

type HGetParam struct {
	Key   string
	field string
}

type HGetResult struct {
	Has   bool
	Value string
}

func HGet(ctx context.Context, param HGetParam) (v HGetResult, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, param)
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, hGetFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	scanErr := result.Scan(&v)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func hGet(ctx context.Context, cmder rds.Cmdable, param HGetParam) (v HGetResult, err error) {
	s := ""
	s, err = cmder.HGet(ctx, param.Key, param.field).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: hget failed").WithCause(err)
		return
	}
	v = HGetResult{
		Has:   true,
		Value: s,
	}
	return
}

const (
	hSetFn = "h_set"
)

type HSetParam struct {
	Key   string
	field string
	value string
}

func HSet(ctx context.Context, param HSetParam) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, param)
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, hSetFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func hSet(ctx context.Context, cmder rds.Cmdable, param HSetParam) (n int64, err error) {
	n, err = cmder.HSet(ctx, param.Key, param.field, param.value).Result()
	if err != nil {
		err = errors.Warning("redis: hset failed").WithCause(err)
		return
	}
	return
}
