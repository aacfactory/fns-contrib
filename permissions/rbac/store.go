package rbac

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

type BindParam struct {
	UserId  string   `json:"userId"`
	RoleIds []string `json:"roleIds"`
}

type SaveRoleParam struct {
	Id          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Parent      string    `json:"parent"`
	Policies    []*Policy `json:"policies"`
}

type Store interface {
	service.Component
	Save(ctx context.Context, param SaveRoleParam) (err errors.CodeError)
	Remove(ctx context.Context, roleId string) (err errors.CodeError)
	Get(ctx context.Context, roleId string) (role Role, err errors.CodeError)
	List(ctx context.Context, roleIds []string) (roles []*Role, err errors.CodeError)
	Bind(ctx context.Context, param BindParam) (err errors.CodeError)
	Bounds(ctx context.Context, userId string) (err errors.CodeError)
}
