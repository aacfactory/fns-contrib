package rbac

import (
	"context"
	"github.com/aacfactory/errors"
)

func Get(ctx context.Context, roleId string) (v Role, err errors.CodeError) {

	return
}

func List(ctx context.Context, roleIds []string) (v []*Role, err errors.CodeError) {

	return
}

func Save(ctx context.Context, param SaveRoleParam) (err errors.CodeError) {

	return
}

func Remove(ctx context.Context, roleId string) (err errors.CodeError) {

	return
}

func Bind(ctx context.Context, param BindParam) (err errors.CodeError) {

	return
}

func Bounds(ctx context.Context, userId string) (v []*Role, err errors.CodeError) {

	return
}
