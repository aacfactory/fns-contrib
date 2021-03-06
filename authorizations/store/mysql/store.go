package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/mysql"
	"github.com/aacfactory/fns/service/builtin/authorizations"
	"github.com/aacfactory/logs"
	"strings"
)

func init() {
	authorizations.RegisterTokenStore(&Store{})
}

type Store struct {
	log    logs.Logger
	Schema string
	Table  string
}

func (store *Store) Build(options authorizations.TokenStoreOptions) (err error) {
	store.log = options.Log
	config := &Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("authorizations mysql store: build failed, decode config failed").WithCause(configErr)
		return
	}
	schema := strings.TrimSpace(config.Schema)
	table := strings.TrimSpace(config.Table)
	if table == "" {
		err = errors.Warning("authorizations mysql store: build failed, table in config is required")
		return
	}
	store.Schema = schema
	store.Table = table
	return
}

func (store *Store) Exist(ctx context.Context, tokenId string) (ok bool) {
	row := TokenRow{
		schema: store.Schema,
		name:   store.Table,
	}
	has, existErr := mysql.Exist(ctx, mysql.NewConditions(mysql.Eq("ID", tokenId)), &row)
	if existErr != nil {
		if store.log.ErrorEnabled() {
			store.log.Error().Caller().Cause(existErr).With("tokenId", tokenId).Message("authorizations mysql store: exist failed")
		}
		return
	}
	ok = has
	return
}

func (store *Store) Save(ctx context.Context, at authorizations.Token) (err error) {
	userId, _ := at.User()
	row := TokenRow{
		Id:        at.Id(),
		UserId:    userId,
		NotBefore: at.NotBefore(),
		NotAfter:  at.NotAfter(),
		Value:     string(at.Bytes()),
		schema:    store.Schema,
		name:      store.Table,
	}
	insertErr := mysql.Insert(ctx, &row)
	if insertErr != nil {
		err = errors.ServiceError("authorizations mysql store: save token failed").WithCause(insertErr)
		return
	}
	return
}

func (store *Store) Remove(ctx context.Context, tokenId string) (err error) {
	row := TokenRow{
		Id:     tokenId,
		schema: store.Schema,
		name:   store.Table,
	}
	rmErr := mysql.Delete(ctx, &row)
	if rmErr != nil {
		err = errors.ServiceError("authorizations mysql store: remove token failed").WithCause(rmErr)
		return
	}
	return
}

func (store *Store) RemoveUserTokens(ctx context.Context, userId string) (err error) {
	query := `DELETE FROM `
	if store.Schema != "" {
		query = query + `"` + store.Schema + `".`
	}
	query = query + `"` + store.Table + `" WHERE "USER_ID" = $1`
	_, _, execErr := mysql.ExecuteContext(ctx, query, userId)
	if execErr != nil {
		err = errors.ServiceError("authorizations mysql store: remove user tokens failed").WithCause(execErr)
		return
	}
	return
}

func (store *Store) Close() (err error) {
	return
}
