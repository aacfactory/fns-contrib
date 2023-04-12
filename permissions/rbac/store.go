package rbac

import (
	"context"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
)

var (
	ErrRoleNofFound             = errors.Warning("rbac: role was not found")
	ErrCantRemoveHasChildrenRow = errors.Warning("rbac: can not remove role which has children")
)

type BindParam struct {
	UserId  string   `json:"userId"`
	RoleIds []string `json:"roleIds"`
}

type SaveRoleParam struct {
	Id          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	ParentId    string    `json:"parentId"`
	Policies    []*Policy `json:"policies"`
}

type StoreOptions struct {
	AppId      string
	AppName    string
	AppVersion versions.Version
	Log        logs.Logger
	Config     configures.Config
}

type Store interface {
	Build(options StoreOptions) (err error)
	Save(ctx context.Context, param SaveRoleParam) (err errors.CodeError)
	Remove(ctx context.Context, roleId string) (err errors.CodeError)
	Get(ctx context.Context, roleId string) (role Role, has bool, err errors.CodeError)
	List(ctx context.Context, roleIds []string) (roles Roles, err errors.CodeError)
	Bind(ctx context.Context, param BindParam) (err errors.CodeError)
	Bounds(ctx context.Context, userId string) (roles Roles, err errors.CodeError)
	Close()
}

const (
	storeComponentName = "store"
)

type storeComponent struct {
	store Store
}

func (component *storeComponent) Name() string {
	return storeComponentName
}

func (component *storeComponent) Build(options service.ComponentOptions) (err error) {
	err = component.store.Build(StoreOptions{
		AppId:      options.AppId,
		AppName:    options.AppName,
		AppVersion: options.AppVersion,
		Log:        options.Log,
		Config:     options.Config,
	})
	return
}

func (component *storeComponent) Close() {
	component.store.Close()
}

func convertStoreToComponent(store Store) service.Component {
	return &storeComponent{
		store: store,
	}
}
