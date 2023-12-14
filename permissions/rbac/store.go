package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
)

var (
	ErrRoleNofFound             = errors.Warning("rbac: role was not found")
	ErrCantRemoveHasChildrenRow = errors.Warning("rbac: can not remove role which has children")
)

type Store interface {
	services.Component
	Role(ctx context.Context, id string) (role Role, has bool, err error)
	Roles(ctx context.Context) (roles Roles, err error)
	SaveRole(ctx context.Context, role Role) (err error)
	RemoveRole(ctx context.Context, role Role) (err error)
	Bind(ctx context.Context, account authorizations.Id, roles []Role) (err error)
	Unbind(ctx context.Context, account authorizations.Id, roles []Role) (err error)
	Bounds(ctx context.Context, account authorizations.Id) (roles Roles, err error)
}
