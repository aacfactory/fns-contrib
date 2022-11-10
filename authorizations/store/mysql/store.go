package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/mysql"
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
	log    logs.Logger
	Schema string
	Table  string
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
		err = errors.Warning("authorizations mysql store: build failed, decode config failed").WithCause(configErr)
		return
	}
	schema := strings.TrimSpace(config.Schema)
	table := strings.TrimSpace(config.Table)
	if table == "" {
		err = errors.Warning("authorizations mysql store: build failed, table in config is required")
		return
	}
	st.Schema = schema
	st.Table = table
	return
}

func (st *store) Exist(ctx context.Context, tokenId string) (ok bool) {
	row := TokenRow{
		schema: st.Schema,
		name:   st.Table,
	}
	has, existErr := mysql.Exist(ctx, mysql.NewConditions(mysql.Eq("ID", tokenId)), &row)
	if existErr != nil {
		if st.log.ErrorEnabled() {
			st.log.Error().Caller().Cause(existErr).With("tokenId", tokenId).Message("authorizations mysql store: exist failed")
		}
		return
	}
	ok = has
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
		schema:    st.Schema,
		name:      st.Table,
	}
	insertErr := mysql.Insert(ctx, &row)
	if insertErr != nil {
		err = errors.ServiceError("authorizations mysql store: save token failed").WithCause(insertErr)
		return
	}
	return
}

func (st *store) Remove(ctx context.Context, tokenId string) (err error) {
	row := TokenRow{
		Id:     tokenId,
		schema: st.Schema,
		name:   st.Table,
	}
	rmErr := mysql.Delete(ctx, &row)
	if rmErr != nil {
		err = errors.ServiceError("authorizations mysql store: remove token failed").WithCause(rmErr)
		return
	}
	return
}

func (st *store) RemoveUserTokens(ctx context.Context, userId string) (err error) {
	query := `DELETE FROM `
	if st.Schema != "" {
		query = query + `"` + st.Schema + `".`
	}
	query = query + `"` + st.Table + `" WHERE "USER_ID" = $1`
	_, _, execErr := mysql.ExecuteContext(ctx, query, userId)
	if execErr != nil {
		err = errors.ServiceError("authorizations mysql store: remove user tokens failed").WithCause(execErr)
		return
	}
	return
}

func (st *store) Close() {
	return
}
