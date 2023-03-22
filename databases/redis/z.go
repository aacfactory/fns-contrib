package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	rds "github.com/redis/go-redis/v9"
)

type Z struct {
	Score float64
	Value string
}

type ZAddParam struct {
	Key    string
	Values []Z
}

const (
	zAddFn = "z_add"
)

func ZAdd(ctx context.Context, param ZAddParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zAddFn, service.NewArgument(pp)))
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

func zAdd(ctx context.Context, cmder rds.Cmdable, param ZAddParam) (n int64, err error) {
	key := param.Key
	members := make([]rds.Z, 0, 1)
	for _, value := range param.Values {
		members = append(members, rds.Z{
			Score:  value.Score,
			Member: value.Value,
		})
	}
	n, err = cmder.ZAdd(ctx, key, members...).Result()
	if err != nil {
		err = errors.Warning("redis: zadd failed").WithCause(err)
		return
	}
	return
}

type ZCardParam struct {
	Key string
}

const (
	zCardFn = "z_card"
)

func ZCard(ctx context.Context, param ZCardParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zCardFn, service.NewArgument(pp)))
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

func zCard(ctx context.Context, cmder rds.Cmdable, param ZCardParam) (n int64, err error) {
	n, err = cmder.ZCard(ctx, param.Key).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: zcard failed").WithCause(err)
		return
	}
	return
}

type ZCountParam struct {
	Key string
	Min string
	Max string
}

const (
	zCountFn = "z_count"
)

func ZCount(ctx context.Context, param ZCountParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zCountFn, service.NewArgument(pp)))
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

func zCount(ctx context.Context, cmder rds.Cmdable, param ZCountParam) (n int64, err error) {
	n, err = cmder.ZCount(ctx, param.Key, param.Min, param.Max).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: zcount failed").WithCause(err)
		return
	}
	return
}

type ZRangeByScoreParam struct {
	Key    string
	Min    string
	Max    string
	Offset int64
	Count  int64
}

const (
	zRangeByScoreFn = "z_range_by_score"
)

func ZRangeByScore(ctx context.Context, param ZRangeByScoreParam) (v []string, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zRangeByScoreFn, service.NewArgument(pp)))
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

func zRangeByScore(ctx context.Context, cmder rds.Cmdable, param ZRangeByScoreParam) (values []string, err error) {
	values, err = cmder.ZRangeByScore(ctx, param.Key, &rds.ZRangeBy{
		Min:    param.Min,
		Max:    param.Max,
		Offset: param.Offset,
		Count:  param.Count,
	}).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: zrangebyscore failed").WithCause(err)
		return
	}
	return
}

type ZRangeParam struct {
	Key   string
	Start int64
	Stop  int64
}

const (
	zRangeFn = "z_range"
)

func ZRange(ctx context.Context, param ZRangeParam) (v []string, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zRangeFn, service.NewArgument(pp)))
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

func zRange(ctx context.Context, cmder rds.Cmdable, param ZRangeParam) (values []string, err error) {
	values, err = cmder.ZRange(ctx, param.Key, param.Start, param.Stop).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: zrange failed").WithCause(err)
		return
	}
	return
}

type ZRemParam struct {
	Key    string
	Values []string
}

const (
	zRemFn = "z_rem"
)

func ZRem(ctx context.Context, param ZRemParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zRemFn, service.NewArgument(pp)))
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

func zRem(ctx context.Context, cmder rds.Cmdable, param ZRemParam) (n int64, err error) {
	values := make([]interface{}, 0, 1)
	for _, value := range param.Values {
		values = append(values, value)
	}
	n, err = cmder.ZRem(ctx, param.Key, values...).Result()
	if err != nil {
		err = errors.Warning("redis: hzrem failed").WithCause(err)
		return
	}
	return
}

type ZRemByRangeParam struct {
	Key   string
	Start int64
	Stop  int64
}

const (
	zRemRangeByRankFn = "z_rem_range_by_rank"
)

func ZRemRangeByRank(ctx context.Context, param ZRemByRangeParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zRemRangeByRankFn, service.NewArgument(pp)))
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

func zRemRangeByRank(ctx context.Context, cmder rds.Cmdable, param ZRemByRangeParam) (n int64, err error) {
	n, err = cmder.ZRemRangeByRank(ctx, param.Key, param.Start, param.Stop).Result()
	if err != nil {
		err = errors.Warning("redis: zremrangebyrank failed").WithCause(err)
		return
	}
	return
}

type ZRemByScoreParam struct {
	Key string
	Min string
	Max string
}

const (
	zRemRangeByScoreFn = "z_rem_range_by_score"
)

func ZRemRangeByScore(ctx context.Context, param ZRemByScoreParam) (v int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, zRemRangeByScoreFn, service.NewArgument(pp)))
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

func zRemRangeByScore(ctx context.Context, cmder rds.Cmdable, param ZRemByScoreParam) (n int64, err error) {
	n, err = cmder.ZRemRangeByScore(ctx, param.Key, param.Min, param.Max).Result()
	if err != nil {
		err = errors.Warning("redis: zremrangebyscore failed").WithCause(err)
		return
	}
	return
}
