package redis

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"time"
)

func Exist(ctx context.Context, key string) (ok bool, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: exist failed").WithCause(paramsErr)
		return
	}
	result, doErr := DoCommand(ctx, Command{
		Name:   "EXISTS",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: exist failed").WithCause(doErr)
		return
	}
	ok = result.Exist
	return
}

func Remove(ctx context.Context, key string) (err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: remove failed").WithCause(paramsErr)
		return
	}
	_, doErr := DoCommand(ctx, Command{
		Name:   "DEL",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: remove failed").WithCause(doErr)
		return
	}
	return
}

func Expire(ctx context.Context, key string, expiration time.Duration) (ok bool, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: expire failed").WithCause(paramsErr)
		return
	}
	paramsErr = params.Append(expiration)
	if paramsErr != nil {
		err = errors.ServiceError("redis: expire failed").WithCause(paramsErr)
		return
	}
	_, doErr := DoCommand(ctx, Command{
		Name:   "EXPIRE",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: set failed").WithCause(doErr)
		return
	}
	ok = true
	return
}

func Persist(ctx context.Context, key string) (ok bool, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: persist failed").WithCause(paramsErr)
		return
	}
	_, doErr := DoCommand(ctx, Command{
		Name:   "PERSIST",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: persist failed").WithCause(doErr)
		return
	}
	ok = true
	return
}

func Set(ctx context.Context, key string, value string, expiration time.Duration) (err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: set failed").WithCause(paramsErr)
		return
	}
	paramsErr = params.Append(value)
	if paramsErr != nil {
		err = errors.ServiceError("redis: set failed").WithCause(paramsErr)
		return
	}
	paramsErr = params.Append(expiration)
	if paramsErr != nil {
		err = errors.ServiceError("redis: set failed").WithCause(paramsErr)
		return
	}
	_, doErr := DoCommand(ctx, Command{
		Name:   "SET",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: set failed").WithCause(doErr)
		return
	}
	return
}

func Get(ctx context.Context, key string) (result *Result, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: get failed").WithCause(paramsErr)
		return
	}
	result, err = DoCommand(ctx, Command{
		Name:   "GET",
		Params: params,
	})
	if err != nil {
		err = errors.ServiceError("redis: get set failed").WithCause(err)
		return
	}
	return
}

func GetSet(ctx context.Context, key string, value string, expiration time.Duration) (result *Result, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: get set failed").WithCause(paramsErr)
		return
	}
	paramsErr = params.Append(value)
	if paramsErr != nil {
		err = errors.ServiceError("redis: get set failed").WithCause(paramsErr)
		return
	}
	paramsErr = params.Append(expiration)
	if paramsErr != nil {
		err = errors.ServiceError("redis: get set failed").WithCause(paramsErr)
		return
	}
	result, err = DoCommand(ctx, Command{
		Name:   "GETSET",
		Params: params,
	})
	if err != nil {
		err = errors.ServiceError("redis: get set set failed").WithCause(err)
		return
	}
	return
}

func Incr(ctx context.Context, key string) (v int64, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: incr failed").WithCause(paramsErr)
		return
	}
	result, doErr := DoCommand(ctx, Command{
		Name:   "INCR",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: incr failed").WithCause(doErr)
		return
	}
	if !result.Exist {
		return
	}
	decodeErr := json.Unmarshal(result.Value, &v)
	if decodeErr != nil {
		err = errors.ServiceError("redis: incr failed").WithCause(decodeErr)
		return
	}
	return
}

func Decr(ctx context.Context, key string) (v int64, err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: decr failed").WithCause(paramsErr)
		return
	}
	result, doErr := DoCommand(ctx, Command{
		Name:   "DECR",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: decr failed").WithCause(doErr)
		return
	}
	if !result.Exist {
		return
	}
	decodeErr := json.Unmarshal(result.Value, &v)
	if decodeErr != nil {
		err = errors.ServiceError("redis: decr failed").WithCause(decodeErr)
		return
	}
	return
}

func Lock(ctx context.Context, key string, expiration time.Duration) (err errors.CodeError) {
	params := Params{}
	var paramsErr errors.CodeError
	paramsErr = params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: lock failed").WithCause(paramsErr)
		return
	}
	paramsErr = params.Append(expiration)
	if paramsErr != nil {
		err = errors.ServiceError("redis: lock failed").WithCause(paramsErr)
		return
	}
	_, doErr := DoCommand(ctx, Command{
		Name:   "SET",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: lock failed").WithCause(doErr)
		return
	}
	return
}

func Unlock(ctx context.Context, key string) (err errors.CodeError) {
	params := Params{}
	paramsErr := params.Append(key)
	if paramsErr != nil {
		err = errors.ServiceError("redis: unlock failed").WithCause(paramsErr)
		return
	}
	_, doErr := DoCommand(ctx, Command{
		Name:   "REMOVE",
		Params: params,
	})
	if doErr != nil {
		err = errors.ServiceError("redis: unlock failed").WithCause(doErr)
		return
	}
	return
}

func DoCommand(ctx context.Context, command Command) (result *Result, err errors.CodeError) {
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.NotFound("redis: endpoint was not found")
		return
	}
	fr := endpoint.Request(ctx, commandFn, service.NewArgument(&command))
	r := Result{}
	_, getResultErr := fr.Get(ctx, &r)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	result = &r
	return
}
