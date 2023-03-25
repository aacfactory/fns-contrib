package sql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/authorizations/tokens"
	db "github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"strings"
	"time"
)

func Store() tokens.Store {
	return &store{}
}

type store struct {
	log      logs.Logger
	database string
}

func (s *store) Name() (name string) {
	name = "sql"
	return
}

func (s *store) Build(options service.ComponentOptions) (err error) {
	s.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("tokens: build failed").WithCause(configErr).WithMeta("store", s.Name())
		return
	}
	s.database = strings.TrimSpace(config.Database)
	_schema = strings.TrimSpace(config.Schema)
	_table = strings.TrimSpace(config.Table)
	if _table == "" {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("table is required")).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Save(ctx context.Context, param tokens.SaveParam) (err errors.CodeError) {
	if param.Id == "" {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("id is required")).WithMeta("store", s.Name())
		return
	}
	if param.UserId == "" {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("user id is required")).WithMeta("store", s.Name())
		return
	}
	if param.Token == "" {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("token is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	insertErr := dal.Insert(ctx, &TokenRow{
		Id:       param.Id,
		UserId:   param.UserId,
		ExpireAT: param.ExpireAT,
		Token:    param.Token,
	})
	if insertErr != nil {
		err = errors.Warning("tokens: save failed").WithCause(insertErr).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Remove(ctx context.Context, param tokens.RemoveParam) (err errors.CodeError) {
	if param.Id != "" {
		if s.database != "" {
			ctx = db.WithOptions(ctx, db.Database(s.database))
		}
		rmErr := dal.Delete(ctx, &TokenRow{
			Id:       param.Id,
			UserId:   "",
			ExpireAT: time.Time{},
			Token:    "",
		})
		if rmErr != nil {
			err = errors.Warning("tokens: remove failed").WithCause(rmErr).WithMeta("store", s.Name())
			return
		}
	} else if param.UserId != "" {
		if s.database != "" {
			ctx = db.WithOptions(ctx, db.Database(s.database))
		}
		ts, listErr := s.List(ctx, param.UserId)
		if listErr != nil {
			err = errors.Warning("tokens: remove failed").WithCause(listErr).WithMeta("store", s.Name())
			return
		}
		if ts == nil || len(ts) == 0 {
			return
		}
		txErr := db.BeginTransaction(ctx)
		if txErr != nil {
			err = errors.Warning("tokens: remove failed").WithCause(txErr).WithMeta("store", s.Name())
			return
		}
		for _, token := range ts {
			rmErr := dal.Delete(ctx, &TokenRow{
				Id:       token.Id,
				UserId:   "",
				ExpireAT: time.Time{},
				Token:    "",
			})
			if rmErr != nil {
				_ = db.RollbackTransaction(ctx)
				err = errors.Warning("tokens: remove failed").WithCause(rmErr).WithMeta("store", s.Name())
				return
			}
		}
		cmtErr := db.CommitTransaction(ctx)
		if cmtErr != nil {
			_ = db.RollbackTransaction(ctx)
			err = errors.Warning("tokens: remove failed").WithCause(cmtErr).WithMeta("store", s.Name())
			return
		}
	} else {
		err = errors.Warning("tokens: remove failed").WithCause(errors.Warning("one of id or user id is required")).WithMeta("store", s.Name())
	}
	return
}

func (s *store) Get(ctx context.Context, id string) (token tokens.Token, err errors.CodeError) {
	if id == "" {
		err = errors.Warning("tokens: get failed").WithCause(errors.Warning("id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	row, getErr := dal.QueryOne[*TokenRow](ctx, dal.NewConditions(dal.Eq("ID", id)))
	if getErr != nil {
		err = errors.Warning("tokens: get failed").WithCause(getErr).WithMeta("store", s.Name())
		return
	}
	token = tokens.Token{
		Id:       row.Id,
		UserId:   row.UserId,
		Token:    row.Token,
		ExpireAT: row.ExpireAT,
	}
	return
}

func (s *store) List(ctx context.Context, userId string) (ts []tokens.Token, err errors.CodeError) {
	if userId == "" {
		err = errors.Warning("tokens: list failed").WithCause(errors.Warning("user id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	rows, listErr := dal.Query[*TokenRow](ctx, dal.NewConditions(dal.Eq("USER_ID", userId)))
	if listErr != nil {
		err = errors.Warning("tokens: list failed").WithCause(listErr).WithMeta("store", s.Name())
		return
	}
	if rows == nil || len(rows) == 0 {
		return
	}
	ts = make([]tokens.Token, 0, len(rows))
	for _, row := range rows {
		ts = append(ts, tokens.Token{
			Id:       row.Id,
			UserId:   row.UserId,
			Token:    row.Token,
			ExpireAT: row.ExpireAT,
		})
	}
	return
}

func (s *store) Close() {
	return
}
