package postgres

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/authorizations"
	"github.com/aacfactory/logs"
	"strings"
)

func Component() (component service.Component) {
	component = &store{}
	return
}

type store struct {
	log          logs.Logger
	databaseName string
}

func (st *store) Name() (name string) {
	name = "store"
	return
}

func (st *store) Build(options service.ComponentOptions) (err error) {
	st.log = options.Log
	config := &Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("authorizations postgres store: build failed, decode config failed").WithCause(configErr)
		return
	}
	st.databaseName = strings.TrimSpace(config.DatabaseName)
	schema := strings.TrimSpace(config.Schema)
	table := strings.TrimSpace(config.Table)
	if table == "" {
		err = errors.Warning("authorizations postgres store: build failed, table in config is required")
		return
	}
	_schema = schema
	_table = table
	return
}

func (st *store) prepare(ctx context.Context) context.Context {
	if st.databaseName == "" {
		return ctx
	}
	return sql.WithOptions(ctx, sql.Database(st.databaseName))
}

func (st *store) Exist(ctx context.Context, tokenId string) (ok bool) {
	ctx = st.prepare(ctx)
	exist, existErr := dal.Exist[*TokenRow](ctx, dal.NewConditions(dal.Eq("ID", tokenId)))
	if existErr != nil {
		if st.log.ErrorEnabled() {
			st.log.Error().Caller().Cause(existErr).With("tokenId", tokenId).Message("authorizations postgres store: exist failed")
		}
		return
	}
	ok = exist
	return
}

func (st *store) Save(ctx context.Context, at authorizations.Token) (err error) {
	userId, _ := at.User()
	row := TokenRow{
		Id:        at.Id(),
		UserId:    userId,
		NotBefore: at.NotBefore(),
		NotAfter:  at.NotAfter(),
		Value:     string(at.Bytes()),
	}
	ctx = st.prepare(ctx)
	insertErr := dal.Insert(ctx, &row)
	if insertErr != nil {
		err = errors.Warning("authorizations postgres store: save token failed").WithCause(insertErr)
		return
	}
	return
}

func (st *store) Remove(ctx context.Context, tokenId string) (err error) {
	row := TokenRow{
		Id: tokenId,
	}
	ctx = st.prepare(ctx)
	rmErr := dal.Delete(ctx, &row)
	if rmErr != nil {
		err = errors.Warning("authorizations postgres store: remove token failed").WithCause(rmErr)
		return
	}
	return
}

func (st *store) RemoveUserTokens(ctx context.Context, userId string) (err error) {
	query := `DELETE FROM `
	if _schema != "" {
		query = query + `"` + _schema + `".`
	}
	query = query + `"` + _table + `" WHERE "USER_ID" = $1`
	ctx = st.prepare(ctx)
	_, _, execErr := sql.Execute(ctx, query, userId)
	if execErr != nil {
		err = errors.Warning("authorizations postgres store: remove user tokens failed").WithCause(execErr)
		return
	}
	return
}

func (st *store) Close() {
	return
}
