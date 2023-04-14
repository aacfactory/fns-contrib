package redis

import (
	"bytes"
	"context"
	"github.com/aacfactory/errors"
	rds "github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns-contrib/permissions/rbac"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/container/trees"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"sort"
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

func (s *store) Build(options rbac.StoreOptions) (err error) {
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
	role, has, getErr := s.Get(ctx, roleId)
	if getErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(getErr).WithMeta("store", s.Name())
		return
	}
	if !has {
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

func (s *store) Get(ctx context.Context, roleId string) (role rbac.Role, has bool, err errors.CodeError) {
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
	has = true
	return
}

func (s *store) List(ctx context.Context, roleIds []string) (roles rbac.Roles, err errors.CodeError) {
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	all, allErr := s.all(ctx)
	if allErr != nil {
		err = errors.Warning("rbac: list failed").WithCause(allErr).WithMeta("store", s.Name()).WithMeta("roleIds", strings.Join(roleIds, ", "))
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
		err = errors.Warning("rbac: list failed").WithCause(err).WithMeta("store", s.Name()).WithMeta("roleIds", strings.Join(roleIds, ", "))
		return
	}
	return
}

func (s *store) Bind(ctx context.Context, param rbac.BindParam) (err errors.CodeError) {
	if param.UserId == "" {
		err = errors.Warning("rbac: bind failed").WithCause(errors.Warning("user id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	if param.RoleIds == nil {
		param.RoleIds = make([]string, 0, 1)
	}
	p, encodeErr := json.Marshal(param.RoleIds)
	if encodeErr != nil {
		err = errors.Warning("rbac: bind failed").WithCause(encodeErr).
			WithMeta("userId", param.UserId).WithMeta("roleIds", strings.Join(param.RoleIds, ", ")).
			WithMeta("store", s.Name())
		return
	}
	setErr := rds.Set(ctx, rds.SetParam{
		Key:        s.buildUserRoleKey(param.UserId),
		Value:      bytex.ToString(p),
		Expiration: 0,
	})
	if encodeErr != nil {
		err = errors.Warning("rbac: bind failed").WithCause(setErr).
			WithMeta("userId", param.UserId).WithMeta("roleIds", strings.Join(param.RoleIds, ", ")).
			WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Bounds(ctx context.Context, userId string) (roles rbac.Roles, err errors.CodeError) {
	if userId == "" {
		err = errors.Warning("rbac: bounds failed").WithCause(errors.Warning("user id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = rds.WithOptions(ctx, rds.Database(s.database))
	}
	value, getErr := rds.Get(ctx, s.buildUserRoleKey(userId))
	if getErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(getErr).WithMeta("store", s.Name()).WithMeta("userId", userId)
		return
	}
	if !value.Has {
		return
	}
	data := bytex.FromString(value.Value)
	if !json.Validate(data) {
		return
	}
	ids := make([]string, 0, 1)
	decodeErr := json.Unmarshal(data, &ids)
	if decodeErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(decodeErr).WithMeta("store", s.Name()).WithMeta("userId", userId)
		return
	}
	if len(ids) == 0 {
		return
	}
	all, allErr := s.all(ctx)
	if allErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(allErr).WithMeta("store", s.Name()).WithMeta("userId", userId)
		return
	}
	if all == nil || len(all) == 0 {
		return
	}
	roles, err = s.mapping(all, ids...)
	if err != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(err).WithMeta("store", s.Name()).WithMeta("userId", userId)
		return
	}
	return
}

func (s *store) all(ctx context.Context) (roles []*rbac.Role, err errors.CodeError) {
	keys, keysErr := rds.Keys(ctx, s.buildRoleKey("*"))
	if keysErr != nil {
		err = errors.Warning("rbac: get role keys failed").WithCause(keysErr)
		return
	}
	roles = make([]*rbac.Role, 0, 1)
	if keys == nil || len(keys) == 0 {
		return
	}
	values, getErr := rds.MGet(ctx, keys)
	if getErr != nil {
		err = errors.Warning("rbac: get role keys failed").WithCause(getErr)
		return
	}
	if values == nil || len(values) == 0 {
		return
	}
	items := make([][]byte, 0, 1)
	for _, key := range keys {
		value, has := values[key]
		if !has {
			continue
		}
		item := bytex.FromString(value)
		if !json.Validate(item) {
			continue
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return
	}
	p := bytes.Join(items, []byte{','})
	roles = make([]*rbac.Role, 0, 1)
	decodeErr := json.Unmarshal(p, &roles)
	if decodeErr != nil {
		err = errors.Warning("rbac: get role failed").WithCause(decodeErr)
		return
	}
	return
}

func (s *store) mapping(all []*rbac.Role, rootIds ...string) (roles rbac.Roles, err errors.CodeError) {
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
		sort.Sort(roles)
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
	sort.Sort(roles)
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
