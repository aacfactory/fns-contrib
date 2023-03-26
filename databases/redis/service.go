package redis

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/internal"
	"github.com/aacfactory/fns/service"
	"strings"
)

const (
	name = "redis"
)

func Service(databases ...string) service.Service {
	components := make([]service.Component, 0, 1)
	if databases == nil || len(databases) == 0 {
		databases = []string{"db"}
	}
	var defaultDB *internal.Database
	for i, database := range databases {
		db := internal.NewDatabase(strings.TrimSpace(database))
		components = append(components, db)
		if i == 0 {
			defaultDB = db
		}
	}
	return &_service_{
		Abstract:  service.NewAbstract(name, true, components...),
		defaultDB: defaultDB,
	}
}

type _service_ struct {
	service.Abstract
	defaultDB *internal.Database
}

func (svc *_service_) Build(options service.Options) (err error) {
	err = svc.Abstract.Build(options)
	if err != nil {
		err = errors.Warning("redis: build failed").WithCause(err)
		return
	}
	svc.Components()
	return
}

func (svc *_service_) Document() (doc service.Document) {
	return
}

func (svc *_service_) database(name string) (db *internal.Database, err error) {
	if name == "" {
		if svc.defaultDB == nil {
			err = errors.Warning("redis: get default database failed")
			return
		}
		db = svc.defaultDB
		return
	}
	v, has := svc.Components()[name]
	if !has {
		err = errors.Warning("redis: database was not found").WithMeta("database", name)
		return
	}
	db, has = v.(*internal.Database)
	if !has {
		err = errors.Warning("redis: database was not found").WithMeta("database", name)
		return
	}
	return
}

func (svc *_service_) Handle(ctx context.Context, fn string, argument service.Argument) (result interface{}, err errors.CodeError) {
	pp := &proxyParam{}
	ppErr := argument.As(pp)
	if ppErr != nil {
		err = errors.BadRequest("redis: invalid command argument").WithCause(ppErr)
		return
	}
	db, dbEr := svc.database(pp.Database)
	if dbEr != nil {
		err = errors.BadRequest("redis: invalid command argument").WithCause(dbEr)
		return
	}
	switch fn {
	case pipelineFn:
		param := PipelineParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		if param.Tx {
			handleErr := db.TxPipeline(ctx)
			if handleErr != nil {
				err = errors.Map(handleErr)
				return
			}
		} else {
			handleErr := db.Pipeline(ctx)
			if handleErr != nil {
				err = errors.Map(handleErr)
				return
			}
		}
		result = pipelineResult{
			Id: svc.AppId(),
		}
		break
	case execFn:
		finished, cmds, execErr := db.Exec(ctx)
		if execErr != nil {
			err = errors.Map(execErr)
			return
		}
		v := ExecResult{
			Finished: finished,
			Cmders:   nil,
		}
		if !finished {
			result = v
			return
		}
		cmders := make([]ExecResultCmder, 0, 1)
		if cmds == nil || len(cmds) == 0 {
			v.Cmders = cmders
			result = v
			return
		}
		for _, cmd := range cmds {
			cmdErr := ""
			if cmd.Err() != nil {
				cmdErr = cmd.Err().Error()
			}
			cmders = append(cmders, ExecResultCmder{
				Name:  cmd.Name(),
				Error: cmdErr,
			})
		}
		v.Cmders = cmders
		result = v
		break
	case discardFn:
		db.Discard(ctx)
	case keysFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		values, handleErr := keys(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = values
		break
	case delFn:
		param := make([]string, 0, 1)
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := del(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case existsFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		has, handleErr := exists(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = has
		break
	case expireFn:
		param := ExpireParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := expire(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case persistFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := persist(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case scanFn:
		param := ScanParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := scan(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case sortFn:
		param := SortParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := sort(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case setFn:
		param := SetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := set(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case setNxFn:
		param := SetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := setNx(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case setExFn:
		param := SetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := setEx(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case getFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := get(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case getSetFn:
		param := GetSetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := getSet(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case mgetFn:
		param := make([]string, 0, 1)
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := mget(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case msetFn:
		param := MSetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		handleErr := mset(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		break
	case incrFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := incr(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case incrByFn:
		param := IncrByParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := incrBy(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case decrFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := decr(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case decrByFn:
		param := DecrByParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := decrBy(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zAddFn:
		param := ZAddParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zAdd(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zCardFn:
		param := ZCardParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zCard(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zCountFn:
		param := ZCountParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zCount(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zRangeByScoreFn:
		param := ZRangeByScoreParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zRangeByScore(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zRangeFn:
		param := ZRangeParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zRange(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zRemFn:
		param := ZRemParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zRem(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zRemRangeByRankFn:
		param := ZRemByRangeParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zRemRangeByRank(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case zRemRangeByScoreFn:
		param := ZRemByScoreParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := zRemRangeByScore(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case hGetALLFn:
		param := ""
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := hGetALL(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case hDelFn:
		param := HDelParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := hDel(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case hExistFn:
		param := HExistParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := hExist(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case hGetFn:
		param := HGetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := hGet(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case hSetFn:
		param := HSetParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := hSet(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case geoAddFn:
		param := GeoAddParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := geoAdd(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case geoDistFn:
		param := GeoDistParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := geoDist(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case geoHashFn:
		param := GeoHashParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := geoHash(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case geoPosFn:
		param := GeoPosParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := geoPos(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case geoSearchFn:
		param := GeoSearchParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := geoSearch(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	case geoSearchStoreFn:
		param := GeoSearchStoreParam{}
		paramErr := pp.ScanPayload(&param)
		if paramErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramErr)
			return
		}
		cmder := db.Cmder(ctx)
		value, handleErr := geoSearchStore(ctx, cmder, param)
		if handleErr != nil {
			err = errors.Map(handleErr)
			return
		}
		result = value
		break
	default:
		err = errors.NotFound("redis: fn was not found").WithMeta("service", name).WithMeta("fn", fn)
		break
	}
	return
}
