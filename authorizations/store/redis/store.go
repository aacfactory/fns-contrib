package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns/service/builtin/authorizations"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"time"
)

func init() {
	authorizations.RegisterTokenStore(&Store{})
}

type Store struct {
	log logs.Logger
}

func (store *Store) Build(options authorizations.TokenStoreOptions) (err error) {
	store.log = options.Log
	return
}

func (store *Store) Exist(ctx context.Context, tokenId string) (ok bool) {
	has, existErr := redis.Exist(ctx, makeKey(tokenId))
	if existErr != nil {
		if store.log.ErrorEnabled() {
			store.log.Error().Caller().Cause(existErr).With("tokenId", tokenId).Message("authorizations redis store: check token exists failed")
		}
		return
	}
	ok = has
	return
}

func (store *Store) Save(ctx context.Context, at authorizations.Token) (err error) {
	expirations := at.NotAfter().Sub(time.Now())
	if expirations < 0 {
		expirations = 0
	}
	key := makeKey(at.Id())
	userId, _ := at.User()
	token := &Token{
		Id:        at.Id(),
		UserId:    userId,
		NotBefore: at.NotBefore(),
		NotAfter:  at.NotAfter(),
		Value:     string(at.Bytes()),
	}
	value, encodeErr := json.Marshal(token)
	if encodeErr != nil {
		err = errors.ServiceError("authorizations redis store: save token failed").WithCause(encodeErr)
		return
	}
	setTokenErr := redis.Set(ctx, key, string(value), expirations)
	if setTokenErr != nil {
		err = errors.ServiceError("authorizations redis store: save token failed").WithCause(setTokenErr)
		return
	}
	userKey := makeUserKey(userId)
	addUserTokenErr := redis.SAdd(ctx, userKey, key)
	if addUserTokenErr != nil {
		err = errors.ServiceError("authorizations redis store: save token failed").WithCause(addUserTokenErr)
		return
	}
	_, _ = redis.Expire(ctx, userKey, expirations)
	return
}

func (store *Store) Remove(ctx context.Context, tokenId string) (err error) {
	key := makeKey(tokenId)
	got, getErr := redis.Get(ctx, key)
	if getErr != nil {
		err = errors.ServiceError("authorizations redis store: remove token failed").WithCause(getErr)
		return
	}
	if !got.Exist {
		return
	}
	token := &Token{}
	decodeErr := got.DecodeJsonValueTo(token)
	if decodeErr != nil {
		err = errors.ServiceError("authorizations redis store: remove token failed").WithCause(decodeErr)
		return
	}
	removeTokenErr := redis.Remove(ctx, key)
	if removeTokenErr != nil {
		if store.log.ErrorEnabled() {
			store.log.Error().Caller().Cause(removeTokenErr).With("tokenId", tokenId).Message("authorizations redis store: remove token failed")
		}
	}
	userKey := makeUserKey(token.UserId)
	remUserTokenErr := redis.SRem(ctx, userKey, key)
	if remUserTokenErr != nil {
		err = errors.ServiceError("authorizations redis store: remove token failed").WithCause(remUserTokenErr)
		return
	}
	return
}

func (store *Store) RemoveUserTokens(ctx context.Context, userId string) (err error) {
	userKey := makeUserKey(userId)
	members, membersErr := redis.SMembers(ctx, userKey)
	if membersErr != nil {
		err = errors.ServiceError("authorizations redis store: remove user tokens failed").WithCause(membersErr)
		return
	}
	if members == nil || len(members) == 0 {
		return
	}
	for _, member := range members {
		_ = redis.Remove(ctx, member)
	}
	return
}

func (store *Store) Close() (err error) {
	return
}

func makeKey(id string) string {
	return fmt.Sprintf("auth_token:%s", id)
}

func makeUserKey(id string) string {
	return fmt.Sprintf("auth_user:%s", id)
}

type Token struct {
	Id        string    `json:"id"`
	UserId    string    `json:"userId"`
	NotBefore time.Time `json:"notBefore"`
	NotAfter  time.Time `json:"notAfter"`
	Value     string    `json:"value"`
}
