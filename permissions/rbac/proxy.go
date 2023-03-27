package rbac

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

func Get(ctx context.Context, roleId string) (v Role, err errors.CodeError) {
	if roleId == "" {
		err = errors.Warning("rbac: get failed").WithCause(errors.Warning("role id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("rbac: get failed").WithCause(errors.Warning("rbac: service was not deployed"))
		return
	}
	future, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, getFn, service.NewArgument(roleId), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	scanErr := future.Scan(&v)
	if scanErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(scanErr)
		return
	}
	return
}

func List(ctx context.Context, roleIds []string) (v Roles, err errors.CodeError) {
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("rbac: list failed").WithCause(errors.Warning("rbac: service was not deployed"))
		return
	}
	future, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, listFn, service.NewArgument(roleIds), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	v = make([]*Role, 0, 1)
	scanErr := future.Scan(&v)
	if scanErr != nil {
		err = errors.Warning("rbac: list failed").WithCause(scanErr)
		return
	}
	return
}

func Save(ctx context.Context, param SaveRoleParam) (err errors.CodeError) {
	if param.Id == "" {
		err = errors.Warning("rbac: save failed").WithCause(errors.Warning("id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("rbac: save failed").WithCause(errors.Warning("rbac: service was not deployed"))
		return
	}
	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, saveFn, service.NewArgument(param), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func Remove(ctx context.Context, roleId string) (err errors.CodeError) {
	if roleId == "" {
		err = errors.Warning("rbac: remove failed").WithCause(errors.Warning("role id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("rbac: remove failed").WithCause(errors.Warning("rbac: service was not deployed"))
		return
	}
	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, removeFn, service.NewArgument(roleId), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func Bind(ctx context.Context, param BindParam) (err errors.CodeError) {
	if param.UserId == "" {
		err = errors.Warning("rbac: bind failed").WithCause(errors.Warning("user id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("rbac: bind failed").WithCause(errors.Warning("rbac: service was not deployed"))
		return
	}
	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, bindFn, service.NewArgument(param), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func Bounds(ctx context.Context, userId string) (v Roles, err errors.CodeError) {
	if userId == "" {
		err = errors.Warning("rbac: bounds failed").WithCause(errors.Warning("user id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("rbac: bounds failed").WithCause(errors.Warning("rbac: service was not deployed"))
		return
	}
	future, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, boundsFn, service.NewArgument(userId), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	v = make([]*Role, 0, 1)
	scanErr := future.Scan(&v)
	if scanErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(scanErr)
		return
	}
	return
}
