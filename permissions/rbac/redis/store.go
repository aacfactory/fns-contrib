package redis

import (
	"context"
	"github.com/aacfactory/errors"
	rds "github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns-contrib/permissions/rbac"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/container/trees"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"strings"
)

const (
	prefix = "fns/rbac"
)

func Store() rbac.Store {
	return &store{}
}

type store struct {
	log      logs.Logger
	database string
	prefix   string
}

func (s *store) Name() (name string) {
	name = "redis"
	return
}

func (s *store) Build(options service.ComponentOptions) (err error) {
	s.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("rbac: build failed").WithCause(configErr).WithMeta("store", s.Name())
		return
	}
	s.database = strings.TrimSpace(config.Database)
	s.prefix = strings.TrimSpace(config.KeyPrefix)
	if s.prefix != "" {
		s.prefix, _ = strings.CutSuffix(s.prefix, "/")
	}
	return
}

func (s *store) buildRoleKey(id string) (key string) {
	if s.prefix == "" {
		key = prefix + "/roles/" + id
	} else {
		key = s.prefix + "/roles/" + id
	}
	return
}

func (s *store) buildUserRoleKey(userId string) (key string) {
	if s.prefix == "" {
		key = prefix + "/users/" + userId
	} else {
		key = s.prefix + "/users/" + userId
	}
	return
}

func (s *store) Close() {
	return
}

func (s *store) Save(ctx context.Context, param rbac.SaveRoleParam) (err errors.CodeError) {
	if param.Id == "" {
		err = errors.Warning("rbac: save failed").WithCause(errors.Warning("id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	role := rbac.Role{
		Id:          param.Id,
		Name:        param.Name,
		Description: param.Description,
		ParentId:    param.ParentId,
		Children:    nil,
		Policies:    param.Policies,
	}
	p, encodeErr := json.Marshal(role)
	if encodeErr != nil {
		err = errors.Warning("rbac: save failed").WithCause(encodeErr).WithMeta("store", s.Name()).WithMeta("id", param.Id)
		return
	}
	setErr := rds.Set(ctx, rds.SetParam{
		Key:        s.buildRoleKey(param.Id),
		Value:      bytex.ToString(p),
		Expiration: 0,
	})
	if setErr != nil {
		err = errors.Warning("rbac: save failed").WithCause(setErr).WithMeta("store", s.Name()).WithMeta("id", param.Id)
		return
	}
	return
}

func (s *store) Remove(ctx context.Context, roleId string) (err errors.CodeError) {
	if roleId == "" {
		err = errors.Warning("rbac: remove failed").WithCause(errors.Warning("roleId is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	role, getErr := s.Get(ctx, roleId)
	if getErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(getErr).WithMeta("store", s.Name())
		return
	}
	if role.Children != nil && len(role.Children) > 0 {
		err = errors.Warning("rbac: remove failed").WithCause(rbac.ErrCantRemoveHasChildrenRow).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	rmErr := rds.Del(ctx, []string{s.buildRoleKey(roleId)})
	if rmErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(rmErr).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	return
}

func (s *store) Get(ctx context.Context, roleId string) (role rbac.Role, err errors.CodeError) {
	if roleId == "" {
		err = errors.Warning("rbac: get failed").WithCause(errors.Warning("role id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	all, allErr := s.all(ctx)
	if allErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(allErr).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	if all == nil || len(all) == 0 {
		err = errors.Warning("rbac: get failed").WithCause(rbac.ErrRoleNofFound).WithMeta("id", roleId).WithMeta("store", s.Name())
		return
	}
	roles, mappingErr := s.mapping(all, roleId)
	if mappingErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(mappingErr).WithMeta("id", roleId).WithMeta("store", s.Name())
		return
	}
	if roles == nil || len(roles) == 0 {
		err = errors.Warning("rbac: get failed").WithCause(rbac.ErrRoleNofFound).WithMeta("id", roleId).WithMeta("store", s.Name())
		return
	}
	role = *roles[0]
	return
}

func (s *store) List(ctx context.Context, roleIds []string) (roles []*rbac.Role, err errors.CodeError) {
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	all, allErr := s.all(ctx)
	if allErr != nil {
		err = errors.Warning("rbac: list failed").WithCause(allErr).WithMeta("store", s.Name()).WithMeta("ids", strings.Join(roleIds, ", "))
		return
	}
	if all == nil || len(all) == 0 {
		return
	}
	if roleIds == nil {
		roleIds = make([]string, 0, 1)
	}
	roles, err = s.mapping(all, roleIds...)
	if err != nil {
		err = errors.Warning("rbac: list failed").WithCause(err).WithMeta("store", s.Name()).WithMeta("ids", strings.Join(roleIds, ", "))
		return
	}
	return
}

func (s *store) Bind(ctx context.Context, param rbac.BindParam) (err errors.CodeError) {
	//TODO implement me
	panic("implement me")
}

func (s *store) Bounds(ctx context.Context, userId string) (roles []*rbac.Role, err errors.CodeError) {
	//TODO implement me
	panic("implement me")
}

func (s *store) all(ctx context.Context) (roles []*rbac.Role, err errors.CodeError) {

	return
}

func (s *store) mapping(all []*rbac.Role, rootIds ...string) (roles []*rbac.Role, err errors.CodeError) {
	if all == nil || len(all) == 0 {
		return
	}
	nodes, convertTreeErr := trees.ConvertListToTree[*rbac.Role](all)
	if convertTreeErr != nil {
		err = errors.Warning("rbac: mapping list to tree failed").WithCause(convertTreeErr)
		return
	}
	if rootIds == nil || len(rootIds) == 0 {
		roles = nodes
		return
	}
	roles = make([]*rbac.Role, 0, 1)
	for _, id := range rootIds {
		for _, node := range nodes {
			target := getRole(node, id)
			if target != nil {
				roles = append(roles, target)
				break
			}
		}
	}
	return
}

func getRole(root *rbac.Role, id string) (role *rbac.Role) {
	if root.Id == id {
		role = root
		return
	}
	if root.Children != nil && len(root.Children) > 0 {
		for _, child := range root.Children {
			role = getRole(child, id)
			if role != nil {
				return
			}
		}
	}
	return
}
