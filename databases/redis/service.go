package redis

import (
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

const (
	namespace = "redis"

	setFn              = "set"
	getFn              = "get"
	getSetFn           = "get_set"
	incrFn             = "incr"
	decrFn             = "decr"
	containsFn         = "contains"
	removeFn           = "remove"
	expireFn           = "expire"
	persistFn          = "persist"
	ttlFn              = "ttl"
	zAddFn             = "z_add"
	zCardFn            = "z_card"
	zRangeFn           = "z_range"
	zRangeByScoreFn    = "z_range_by_score"
	zRemFn             = "z_rem"
	zRemByRankFn       = "z_rem_by_rank"
	zRemByScoreFn      = "z_rem_by_score"
	zRevRangeFn        = "z_rev_range"
	zRevRangeByScoreFn = "z_rev_range_by_score"
	lockFn             = "lock"
	unlockFn           = "unlock"
	originCmdFn        = "cmd"
)

func Service() fns.Service {
	return &service{}
}

type service struct {
	client Client
}

func (svc *service) Namespace() string {
	return namespace
}

func (svc *service) Internal() bool {
	return true
}

func (svc *service) Build(_ fns.Context, root configuares.Config) (err error) {
	config := Config{}
	readErr := root.As(&config)
	if readErr != nil {
		err = fmt.Errorf("fns Redis Build: read redis config failed, %v", readErr)
		return
	}

	client, createErr := config.CreateClient()
	if createErr != nil {
		err = createErr
		return
	}

	svc.client = client
	return
}

func (svc *service) Document() (doc *fns.ServiceDocument) {
	return
}

func (svc *service) Handle(context fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	switch fn {
	case setFn:
		context = fns.WithFn(context, fn)
		param := SetParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.set(context, param)
		if err == nil {
			result = struct{}{}
		}
	case getFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.get(context, key)
	case getSetFn:
		context = fns.WithFn(context, fn)
		param := SetParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.getAndSet(context, param)
	case incrFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.incr(context, key)
		if err == nil {
			result = struct{}{}
		}
	case decrFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.decr(context, key)
		if err == nil {
			result = struct{}{}
		}
	case containsFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.contains(context, key)
	case removeFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.remove(context, key)
		if err == nil {
			result = struct{}{}
		}
	case expireFn:
		context = fns.WithFn(context, fn)
		param := ExpireParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.expire(context, param)
	case persistFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.persist(context, key)
	case ttlFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.ttl(context, key)
	case zAddFn:
		context = fns.WithFn(context, fn)
		param := ZAddParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.zAdd(context, param)
		if err == nil {
			result = struct{}{}
		}
	case zCardFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zCard(context, key)
	case zRangeFn:
		context = fns.WithFn(context, fn)
		param := ZRangeParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRange(context, param)
	case zRangeByScoreFn:
		context = fns.WithFn(context, fn)
		param := ZRangeByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRangeByScore(context, param)
	case zRemFn:
		context = fns.WithFn(context, fn)
		param := ZRemParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRem(context, param)
	case zRemByRankFn:
		context = fns.WithFn(context, fn)
		param := ZRemByRankParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRemByRank(context, param)
	case zRemByScoreFn:
		context = fns.WithFn(context, fn)
		param := ZRemByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRemByScore(context, param)
	case zRevRangeFn:
		context = fns.WithFn(context, fn)
		param := ZRevRangeParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRevRange(context, param)
	case zRevRangeByScoreFn:
		context = fns.WithFn(context, fn)
		param := ZRevRangeByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRevRangeByScore(context, param)
	case lockFn:
		context = fns.WithFn(context, fn)
		param := LockParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.lock(context, param)
		if err == nil {
			result = struct{}{}
		}
	case unlockFn:
		context = fns.WithFn(context, fn)
		param := ""
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.unlock(context, param)
		if err == nil {
			result = struct{}{}
		}
	case originCmdFn:
		context = fns.WithFn(context, fn)
		param := OriginCommandArg{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.cmd(context, param)
	default:
		err = errors.NotFound(fmt.Sprintf("fns Redis: %s was not found", fn))
	}
	return
}

func (svc *service) Shutdown() (err error) {
	err = svc.client.Close()
	return
}
