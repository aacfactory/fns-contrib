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

type SetParam struct {
	Key        string        `json:"key"`
	Value      string        `json:"value"`
	Expiration time.Duration `json:"expiration"`
}

const (
	setFn = "set"
)

func Set(ctx context.Context, param SetParam) (err errors.CodeError) {
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, setFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func set(ctx context.Context, cmder rds.Cmdable, param SetParam) (err error) {
	err = cmder.Set(ctx, param.Key, param.Value, param.Expiration).Err()
	if err != nil {
		err = errors.Warning("redis: set failed").WithCause(err)
		return
	}
	return
}

const (
	setNxFn = "set_nx"
)

func SetNx(ctx context.Context, param SetParam) (err errors.CodeError) {
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, setNxFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func setNx(ctx context.Context, cmder rds.Cmdable, param SetParam) (err error) {
	err = cmder.SetNX(ctx, param.Key, param.Value, param.Expiration).Err()
	if err != nil {
		err = errors.Warning("redis: setnx failed").WithCause(err)
		return
	}
	return
}

const (
	setExFn = "set_ex"
)

func SetEx(ctx context.Context, param SetParam) (err errors.CodeError) {
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, setExFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func setEx(ctx context.Context, cmder rds.Cmdable, param SetParam) (err error) {
	_, err = cmder.SetEx(ctx, param.Key, param.Value, param.Expiration).Result()
	if err != nil {
		err = errors.Warning("redis: setex failed").WithCause(err)
		return
	}
	return
}

type GetResult struct {
	Has   bool   `json:"has"`
	Value string `json:"value"`
}

const (
	getFn = "get"
)

func Get(ctx context.Context, key string) (v GetResult, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, getFn, service.NewArgument(param)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	v = GetResult{}
	scanErr := result.Scan(&v)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func get(ctx context.Context, cmder rds.Cmdable, key string) (v GetResult, err error) {
	s := ""
	s, err = cmder.Get(ctx, key).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: get failed").WithCause(err)
		return
	}
	v = GetResult{
		Has:   true,
		Value: s,
	}
	return
}

type GetSetParam struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

const (
	getSetFn = "get_set"
)

func GetSet(ctx context.Context, param GetSetParam) (v GetResult, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, getSetFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	v = GetResult{}
	scanErr := result.Scan(&v)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func getSet(ctx context.Context, cmder rds.Cmdable, param GetSetParam) (v GetResult, err error) {
	s := ""
	s, err = cmder.GetSet(ctx, param.Key, param.Value).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: getset failed").WithCause(err)
		return
	}
	v = GetResult{
		Has:   true,
		Value: s,
	}
	return
}

const (
	mgetFn = "mget"
)

func MGet(ctx context.Context, keys []string) (v map[string]string, err errors.CodeError) {
	opt := getOptions(ctx)
	database := opt.database
	pp, paramErr := newProxyParam(database, keys)
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, mgetFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if !result.Exist() {
		return
	}
	v = make(map[string]string)
	scanErr := result.Scan(&v)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func mget(ctx context.Context, cmder rds.Cmdable, keys []string) (values map[string]string, err error) {
	results := make([]interface{}, 0, 1)
	results, err = cmder.MGet(ctx, keys...).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: mget failed").WithCause(err)
		return
	}
	values = make(map[string]string)
	for i, key := range keys {
		result := results[i]
		if result == nil || result == rds.Nil {
			continue
		}
		rs, ok := result.(string)
		if ok {
			values[key] = rs
		}
	}
	return
}

type MSetParam []KeyPair

type KeyPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

const (
	msetFn = "mset"
)

func MSet(ctx context.Context, param MSetParam) (err errors.CodeError) {
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, msetFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func mset(ctx context.Context, cmder rds.Cmdable, param MSetParam) (err error) {
	params := make([]interface{}, 0, 1)
	for _, pair := range param {
		params = append(params, pair.Key, pair.Value)
	}
	_, err = cmder.MSet(ctx, params...).Result()
	if err != nil {
		err = errors.Warning("redis: mset failed").WithCause(err)
		return
	}
	return
}

const (
	incrFn = "incr"
)

func Incr(ctx context.Context, key string) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, incrFn, service.NewArgument(pp)))
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

func incr(ctx context.Context, cmder rds.Cmdable, key string) (n int64, err error) {
	n, err = cmder.Incr(ctx, key).Result()
	if err != nil {
		err = errors.Warning("redis: incr failed").WithCause(err)
		return
	}
	return
}

type IncrByParam struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

const (
	incrByFn = "incr_by"
)

func IncrBy(ctx context.Context, param IncrByParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, incrByFn, service.NewArgument(pp)))
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

func incrBy(ctx context.Context, cmder rds.Cmdable, param IncrByParam) (n int64, err error) {
	n, err = cmder.IncrBy(ctx, param.Key, param.Value).Result()
	if err != nil {
		err = errors.Warning("redis: incrby failed").WithCause(err)
		return
	}
	return
}

const (
	decrFn = "decr"
)

func Decr(ctx context.Context, key string) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, decrFn, service.NewArgument(pp)))
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

func decr(ctx context.Context, cmder rds.Cmdable, key string) (n int64, err error) {
	n, err = cmder.Decr(ctx, key).Result()
	if err != nil {
		err = errors.Warning("redis: decr failed").WithCause(err)
		return
	}
	return
}

type DecrByParam struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

const (
	decrByFn = "decr_by"
)

func DecrBy(ctx context.Context, param DecrByParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, decrByFn, service.NewArgument(pp)))
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

func decrBy(ctx context.Context, cmder rds.Cmdable, param DecrByParam) (n int64, err error) {
	n, err = cmder.DecrBy(ctx, param.Key, param.Value).Result()
	if err != nil {
		err = errors.Warning("redis: decrby failed").WithCause(err)
		return
	}
	return
}
