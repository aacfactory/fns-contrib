package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	rds "github.com/redis/go-redis/v9"
)

type GeoLocation struct {
	Name      string  `json:"name"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Dist      float64 `json:"dist"`
	GeoHash   int64   `json:"geoHash"`
}

const (
	geoAddFn = "geo_add"
)

type GeoAddParam struct {
	Key       string         `json:"key"`
	Locations []*GeoLocation `json:"locations"`
}

func GeoAdd(ctx context.Context, param GeoAddParam) (err errors.CodeError) {
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

	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, geoAddFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func geoAdd(ctx context.Context, cmder rds.Cmdable, param GeoAddParam) (n int64, err error) {
	locations := make([]*rds.GeoLocation, 0, 1)
	if param.Locations == nil {
		err = errors.Warning("redis: geoadd failed").WithCause(errors.Warning("locations is required"))
		return
	}
	for _, location := range param.Locations {
		locations = append(locations, &rds.GeoLocation{
			Name:      location.Name,
			Longitude: location.Longitude,
			Latitude:  location.Latitude,
			Dist:      location.Dist,
			GeoHash:   location.GeoHash,
		})
	}
	n, err = cmder.GeoAdd(ctx, param.Key, locations...).Result()
	if err != nil {
		err = errors.Warning("redis: geoadd failed").WithCause(err)
		return
	}
	return
}

const (
	geoDistFn = "geo_dist"
)

type GeoDistParam struct {
	Key        string `json:"key"`
	SourceName string `json:"sourceName"`
	TargetName string `json:"targetName"`
	Unit       string `json:"unit"`
}

func GeoDist(ctx context.Context, param GeoDistParam) (dist float64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, geoDistFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if result.Exist() {
		scanErr := result.Scan(&dist)
		if scanErr != nil {
			err = scanErr
			return
		}
	}
	return
}

func geoDist(ctx context.Context, cmder rds.Cmdable, param GeoDistParam) (n float64, err error) {
	n, err = cmder.GeoDist(ctx, param.Key, param.SourceName, param.TargetName, param.Unit).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: geodist failed").WithCause(err)
		return
	}
	return
}

const (
	geoHashFn = "geo_hash"
)

type GeoHashParam struct {
	Key   string   `json:"key"`
	Names []string `json:"names"`
}

func GeoHash(ctx context.Context, param GeoDistParam) (n []string, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, geoHashFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if result.Exist() {
		n = make([]string, 0, 1)
		scanErr := result.Scan(&n)
		if scanErr != nil {
			err = scanErr
			return
		}
	}
	return
}

func geoHash(ctx context.Context, cmder rds.Cmdable, param GeoHashParam) (n []string, err error) {
	n, err = cmder.GeoHash(ctx, param.Key, param.Names...).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: geohash failed").WithCause(err)
		return
	}
	return
}

const (
	geoPosFn = "geo_pos"
)

type GeoPosParam struct {
	Key   string   `json:"key"`
	Names []string `json:"names"`
}

type GeoPosition struct {
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
}

func GeoPos(ctx context.Context, param GeoPosParam) (n []*GeoPosition, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, geoPosFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if result.Exist() {
		n = make([]*GeoPosition, 0, 1)
		scanErr := result.Scan(&n)
		if scanErr != nil {
			err = scanErr
			return
		}
	}
	return
}

func geoPos(ctx context.Context, cmder rds.Cmdable, param GeoPosParam) (n []*GeoPosition, err error) {
	var values []*rds.GeoPos
	values, err = cmder.GeoPos(ctx, param.Key, param.Names...).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: geopos failed").WithCause(err)
		return
	}
	if values == nil {
		return
	}
	n = make([]*GeoPosition, 0, 1)
	for _, value := range values {
		n = append(n, &GeoPosition{
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
		})
	}
	return
}

const (
	geoSearchFn = "geo_search"
)

type GeoSearchParam struct {
	Key   string                      `json:"key"`
	Query *rds.GeoSearchLocationQuery `json:"query"`
}

func GeoSearch(ctx context.Context, param GeoSearchParam) (n []*GeoLocation, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, geoSearchFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if result.Exist() {
		n = make([]*GeoLocation, 0, 1)
		scanErr := result.Scan(&n)
		if scanErr != nil {
			err = scanErr
			return
		}
	}
	return
}

func geoSearch(ctx context.Context, cmder rds.Cmdable, param GeoSearchParam) (n []*GeoLocation, err error) {
	var values []rds.GeoLocation
	values, err = cmder.GeoSearchLocation(ctx, param.Key, param.Query).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: geosearch failed").WithCause(err)
		return
	}
	if values == nil {
		return
	}
	n = make([]*GeoLocation, 0, 1)
	for _, value := range values {
		n = append(n, &GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		})
	}
	return
}

const (
	geoSearchStoreFn = "geo_search_store"
)

type GeoSearchStoreParam struct {
	Key   string                   `json:"key"`
	Store string                   `json:"store"`
	Query *rds.GeoSearchStoreQuery `json:"query"`
}

func GeoSearchStore(ctx context.Context, param GeoSearchStoreParam) (n int64, err errors.CodeError) {
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

	result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, geoSearchStoreFn, service.NewArgument(pp)))
	if requestErr != nil {
		err = requestErr
		return
	}
	if result.Exist() {
		scanErr := result.Scan(&n)
		if scanErr != nil {
			err = scanErr
			return
		}
	}
	return
}

func geoSearchStore(ctx context.Context, cmder rds.Cmdable, param GeoSearchStoreParam) (n int64, err error) {
	n, err = cmder.GeoSearchStore(ctx, param.Key, param.Store, param.Query).Result()
	if err != nil {
		if err == rds.Nil {
			return
		}
		err = errors.Warning("redis: geosearchstore failed").WithCause(err)
		return
	}
	return
}
