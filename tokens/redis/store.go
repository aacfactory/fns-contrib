package redis

import (
	"context"
	"github.com/aacfactory/errors"
	rds "github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns-contrib/tokens"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"strings"
	"time"
)

const (
	prefix = "fns/tokens"
)

func Store() tokens.Store {
	return &store{}
}

type store struct {
	log      logs.Logger
	database string
	prefix   string
}

func (s *store) Name() (name string) {
	name = "redis"
	return
}

func (s *store) Build(options tokens.StoreOptions) (err error) {
	s.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("tokens: build failed").WithCause(configErr).WithMeta("store", s.Name())
		return
	}
	s.database = strings.TrimSpace(config.Database)
	s.prefix = strings.TrimSpace(config.KeyPrefix)
	if s.prefix != "" {
		s.prefix, _ = strings.CutSuffix(s.prefix, "/")
	}
	return
}

func (s *store) buildUserKey(userId service.RequestUserId) (key string) {
	if s.prefix == "" {
		key = prefix + "/" + userId.String()
	} else {
		key = s.prefix + "/" + userId.String()
	}
	return
}

func (s *store) buildUserTokenKey(userId string, tokenId string) (key string) {
	if s.prefix == "" {
		key = prefix + "/" + userId + "/" + tokenId
	} else {
		key = s.prefix + "/" + userId + "/" + tokenId
	}
	return
}

func (s *store) Save(ctx context.Context, param tokens.SaveParam) (err errors.CodeError) {
	expiration := time.Duration(0)
	if !param.ExpireAT.IsZero() {
		expiration = param.ExpireAT.Sub(time.Now())
	}
	if expiration <= 0 {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("expire is out of date")).WithMeta("store", s.Name())
		return
	}
	p, encodeErr := json.Marshal(tokens.Token{
		Id:       param.Id,
		UserId:   param.UserId,
		Token:    param.Token,
		ExpireAT: param.ExpireAT,
	})
	if encodeErr != nil {
		err = errors.Warning("tokens: save failed").WithCause(encodeErr).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	setErr := rds.Set(ctx, rds.SetParam{
		Key:        s.buildUserTokenKey(param.UserId, param.Id),
		Value:      bytex.ToString(p),
		Expiration: expiration,
	})
	if setErr != nil {
		err = errors.Warning("tokens: save failed").WithCause(setErr).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Remove(ctx context.Context, param tokens.RemoveParam) (err errors.CodeError) {
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	err = rds.Del(ctx, []string{s.buildUserTokenKey(param.UserId, param.Id)})
	if err != nil {
		err = errors.Warning("tokens: remove failed").WithCause(err).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Get(ctx context.Context, id string) (token tokens.Token, has bool, err errors.CodeError) {
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	keys, keysErr := rds.Keys(ctx, s.buildUserTokenKey("*", id))
	if keysErr != nil {
		err = errors.Warning("tokens: get failed").WithCause(keysErr).WithMeta("store", s.Name())
		return
	}
	if keys == nil || len(keys) == 0 {
		return
	}
	if len(keys) > 1 {
		err = errors.Warning("tokens: get failed").WithCause(errors.Warning("too many tokens")).WithMeta("store", s.Name())
		return
	}
	key := keys[0]
	r, getErr := rds.Get(ctx, key)
	if getErr != nil {
		err = errors.Warning("tokens: get failed").WithCause(getErr).WithMeta("store", s.Name())
		return
	}
	if !r.Has {
		return
	}
	decodeErr := json.Unmarshal(bytex.FromString(r.Value), &token)
	if decodeErr != nil {
		err = errors.Warning("tokens: get failed").WithCause(decodeErr).WithMeta("store", s.Name())
		return
	}
	has = true
	return
}

func (s *store) List(ctx context.Context, userId string) (v []tokens.Token, err errors.CodeError) {
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	keys, keysErr := rds.Keys(ctx, s.buildUserTokenKey(userId, "*"))
	if keysErr != nil {
		err = errors.Warning("tokens: list failed").WithCause(keysErr).WithMeta("store", s.Name())
		return
	}
	if keys == nil || len(keys) == 0 {
		return
	}
	values, getErr := rds.MGet(ctx, keys)
	if getErr != nil {
		err = errors.Warning("tokens: list failed").WithCause(getErr).WithMeta("store", s.Name())
		return
	}
	if values == nil || len(values) == 0 {
		return
	}
	v = make([]tokens.Token, 0, 1)
	for _, key := range keys {
		value, has := values[key]
		if !has {
			continue
		}
		token := tokens.Token{}
		decodeErr := json.Unmarshal(bytex.FromString(value), &token)
		if decodeErr != nil {
			err = errors.Warning("tokens: list failed").WithCause(decodeErr).WithMeta("store", s.Name())
			return
		}
		v = append(v, token)
	}
	return
}

func (s *store) Close() {
	return
}
