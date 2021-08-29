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

type Service struct {
	client Client
}

func (svc *Service) Namespace() string {
	return Namespace
}

func (svc *Service) Internal() bool {
	return true
}

func (svc *Service) Build(root configuares.Config) (err error) {
	config := Config{}
	has, readErr := root.Get(configPath, &config)
	if readErr != nil {
		err = fmt.Errorf("fns Redis Service Build: read redis config failed, %v", readErr)
		return
	}
	if !has {
		err = fmt.Errorf("fns Redis Service Build: no redis path in root config")
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

func (svc *Service) Description() (description []byte) {
	return
}

func (svc *Service) Handle(context fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	switch fn {
	case SetFn:
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
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.get(context, key)
	case IncrFn:
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
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.contains(context, key)
	case RemoveFn:
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.remove(context, key)
	case ExpireFn:
		param := ExpireParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.expire(context, param)
	case PersistFn:
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.persist(context, key)
	case TTLFn:
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.ttl(context, key)
	case ZAddFn:
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
		key := ""
		argErr := argument.As(&key)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zCard(context, key)
	case ZRangeFn:
		param := ZRangeParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRange(context, param)
	case ZRangeByScoreFn:
		param := ZRangeByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRangeByScore(context, param)
	case ZRemFn:
		param := ZRemParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRem(context, param)
	case ZRemByRankFn:
		param := ZRemByRankParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRemByRank(context, param)
	case ZRemByScoreFn:
		param := ZRemByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRemByScore(context, param)
	case ZRevRangeFn:
		param := ZRevRangeParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRevRange(context, param)
	case ZRevRangeByScoreFn:
		param := ZRevRangeByScoreParam{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.zRevRangeByScore(context, param)
	case LockFn:
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
		param := OriginCommandArg{}
		argErr := argument.As(&param)
		if argErr != nil {
			err = argErr
			return
		}
		result, err = svc.cmd(context, param)
	default:
		err = errors.NotFound(fmt.Sprintf("fns Redis Service: %s was not found", fn))
	}
	return
}

func (svc *Service) Close() (err error) {
	err = svc.client.Close()
	return
}
