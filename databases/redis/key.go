package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	rds "github.com/redis/go-redis/v9"
	"time"
)

type KeysParam struct {
	Pattern string `json:"pattern"`
}

type KeysResult struct {
	Values []string `json:"values"`
}

const (
	keysFn = "keys"
)

func Keys(ctx context.Context, pattern string) (values []string, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	param, paramErr := newProxyParam(database, pattern)
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, keysFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	values = make([]string, 0, 1)
	scanErr := result.Scan(&values)
	if scanErr != nil {
		err = scanErr
		return
	}

	return
}

func keys(ctx context.Context, cmder rds.Cmdable, pattern string) (values []string, err error) {
	values, err = cmder.Keys(ctx, pattern).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: keys failed").WithCause(err)
		return
	}
	return
}

const (
	delFn = "del"
)

func Del(ctx context.Context, keys []string) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	param, paramErr := newProxyParam(database, keys)
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, delFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}

	return
}

func del(ctx context.Context, cmder rds.Cmdable, keys []string) (err error) {
	err = cmder.Del(ctx, keys...).Err()
	if err != nil {
		err = errors.Warning("redis: del failed").WithCause(err)
		return
	}
	return
}

const (
	existsFn = "exists"
)

func Exists(ctx context.Context, key string) (has bool, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	param, paramErr := newProxyParam(database, key)
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, existsFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	scanErr := result.Scan(&has)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func exists(ctx context.Context, cmder rds.Cmdable, key string) (has bool, err error) {
	n := int64(0)
	n, err = cmder.Exists(ctx, key).Result()
	if err != nil {
		err = errors.Warning("redis: exists failed").WithCause(err)
		return
	}
	has = n > 0
	return
}

type ExpireParam struct {
	Key        string        `json:"key"`
	Expiration time.Duration `json:"expiration"`
}

const (
	expireFn = "expire"
)

func Expire(ctx context.Context, param ExpireParam) (err errors.CodeError) {
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, expireFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func expire(ctx context.Context, cmder rds.Cmdable, param ExpireParam) (err error) {
	_, err = cmder.Expire(ctx, param.Key, param.Expiration).Result()
	if err != nil {
		err = errors.Warning("redis: expire failed").WithCause(err)
		return
	}
	return
}

const (
	persistFn = "persist"
)

func Persist(ctx context.Context, key string) (err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, key)
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, expireFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func persist(ctx context.Context, cmder rds.Cmdable, key string) (err error) {
	_, err = cmder.Persist(ctx, key).Result()
	if err != nil {
		err = errors.Warning("redis: persist failed").WithCause(err)
		return
	}
	return
}

type ScanParam struct {
	Cursor uint64 `json:"cursor"`
	Match  string `json:"match"`
	Count  int64  `json:"count"`
}

type ScanResult struct {
	Keys []string `json:"keys"`
	Next uint64   `json:"next"`
}

const (
	scanFn = "scan"
)

func Scan(ctx context.Context, param ScanParam) (r ScanResult, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, scanFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	r = ScanResult{}
	scanErr := result.Scan(&r)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func scan(ctx context.Context, cmder rds.Cmdable, param ScanParam) (result ScanResult, err error) {
	result = ScanResult{}
	result.Keys, result.Next, err = cmder.Scan(ctx, param.Cursor, param.Match, param.Count).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: scan failed").WithCause(err)
		return
	}
	return
}

type SortParam struct {
	Key    string   `json:"key"`
	By     string   `json:"by"`
	Offset int64    `json:"offset"`
	Count  int64    `json:"count"`
	Get    []string `json:"get"`
	Order  string   `json:"order"`
	Alpha  bool     `json:"alpha"`
}

const (
	sortFn = "sort"
)

func Sort(ctx context.Context, param SortParam) (v []string, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, sortFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	v = make([]string, 0, 1)
	scanErr := result.Scan(&v)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func sort(ctx context.Context, cmder rds.Cmdable, param SortParam) (v []string, err error) {
	opt := &rds.Sort{
		By:     param.By,
		Offset: param.Offset,
		Count:  param.Count,
		Get:    param.Get,
		Order:  param.Order,
		Alpha:  param.Alpha,
	}
	v, err = cmder.Sort(ctx, param.Key, opt).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: sort failed").WithCause(err)
		return
	}
	return
}
