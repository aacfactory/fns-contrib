package redis

import (
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

const (
	configPath = "redis"

	Namespace = "redis"

	SetFn              = "set"
	GetFn              = "get"
	IncrFn             = "incr"
	DecrFn             = "decr"
	ContainsFn         = "contains"
	RemoveFn           = "remove"
	ExpireFn           = "expire"
	PersistFn          = "persist"
	TTLFn              = "ttl"
	ZAddFn             = "z_add"
	ZCardFn            = "z_card"
	ZRangeFn           = "z_range"
	ZRangeByScoreFn    = "z_range_by_score"
	ZRemFn             = "z_rem"
	ZRemByRankFn       = "z_rem_by_rank"
	ZRemByScoreFn      = "z_rem_by_score"
	ZRevRangeFn        = "z_rev_range"
	ZRevRangeByScoreFn = "z_rev_range_by_score"
	LockFn             = "lock"
	UnlockFn           = "unlock"
	OriginCmdFn        = "cmd"
)

type _service struct {
	client Client
}

func (svc *_service) Namespace() string {
	return Namespace
}

func (svc *_service) Internal() bool {
	return true
}

func (svc *_service) Build(root configuares.Config) (err error) {
	config := Config{}
	has, readErr := root.Get(configPath, &config)
	if readErr != nil {
		err = fmt.Errorf("fns Redis Build: read redis config failed, %v", readErr)
		return
	}
	if !has {
		err = fmt.Errorf("fns Redis Build: no redis path in root config")
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

func (svc *_service) Description() (description []byte) {
	return
}

func (svc *_service) Handle(context fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	switch fn {
	case SetFn:
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
	case GetFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.get(context, key)
	case IncrFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.incr(context, key)
		if err == nil {
			result = struct{}{}
		}
	case DecrFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		err = svc.decr(context, key)
		if err == nil {
			result = struct{}{}
		}
	case ContainsFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.contains(context, key)
	case RemoveFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.remove(context, key)
	case ExpireFn:
		context = fns.WithFn(context, fn)
		param := ExpireParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.expire(context, param)
	case PersistFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.persist(context, key)
	case TTLFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.ttl(context, key)
	case ZAddFn:
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
	case ZCardFn:
		context = fns.WithFn(context, fn)
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zCard(context, key)
	case ZRangeFn:
		context = fns.WithFn(context, fn)
		param := ZRangeParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRange(context, param)
	case ZRangeByScoreFn:
		context = fns.WithFn(context, fn)
		param := ZRangeByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRangeByScore(context, param)
	case ZRemFn:
		context = fns.WithFn(context, fn)
		param := ZRemParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRem(context, param)
	case ZRemByRankFn:
		context = fns.WithFn(context, fn)
		param := ZRemByRankParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRemByRank(context, param)
	case ZRemByScoreFn:
		context = fns.WithFn(context, fn)
		param := ZRemByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRemByScore(context, param)
	case ZRevRangeFn:
		context = fns.WithFn(context, fn)
		param := ZRevRangeParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRevRange(context, param)
	case ZRevRangeByScoreFn:
		context = fns.WithFn(context, fn)
		param := ZRevRangeByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRevRangeByScore(context, param)
	case LockFn:
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
	case UnlockFn:
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
	case OriginCmdFn:
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

func (svc *_service) Close() (err error) {
	err = svc.client.Close()
	return
}
