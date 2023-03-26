package sql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/permissions/rbac"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"strings"
)

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

	_roleSchema = strings.TrimSpace(config.RoleTable.Schema)
	_roleTable = strings.TrimSpace(config.RoleTable.Table)
	if _roleTable == "" {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("role table is required")).WithMeta("store", s.Name())
		return
	}
	_userSchema = strings.TrimSpace(config.UserTable.Schema)
	_userTable = strings.TrimSpace(config.UserTable.Table)
	if _userTable == "" {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("user table is required")).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Close() {
	return
}

func (s *store) Save(ctx context.Context, param rbac.SaveRoleParam) (err errors.CodeError) {

	return
}

func (s *store) Remove(ctx context.Context, roleId string) (err errors.CodeError) {
	return
}

func (s *store) Get(ctx context.Context, roleId string) (role rbac.Role, err errors.CodeError) {
	return
}

func (s *store) List(ctx context.Context, roleIds []string) (roles []*rbac.Role, err errors.CodeError) {
	return
}

func (s *store) Bind(ctx context.Context, param rbac.BindParam) (err errors.CodeError) {
	return
}

func (s *store) Bounds(ctx context.Context, userId string) (err errors.CodeError) {
	return
}
