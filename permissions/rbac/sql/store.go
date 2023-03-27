package sql

import (
	"context"
	"github.com/aacfactory/copier"
	"github.com/aacfactory/errors"
	db "github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/fns-contrib/permissions/rbac"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"sort"
	"strings"
	"time"
)

func Store() rbac.Store {
	return &store{
		log:      nil,
		database: "",
	}
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
		err = errors.Warning("rbac: build failed").WithCause(configErr).WithMeta("store", s.Name())
		return
	}
	s.database = strings.TrimSpace(config.Database)

	_roleSchema = strings.TrimSpace(config.RoleTable.Schema)
	_roleTable = strings.TrimSpace(config.RoleTable.Table)
	if _roleTable == "" {
		err = errors.Warning("rbac: build failed").WithCause(errors.Warning("role table is required")).WithMeta("store", s.Name())
		return
	}
	_userSchema = strings.TrimSpace(config.UserTable.Schema)
	_userTable = strings.TrimSpace(config.UserTable.Table)
	if _userTable == "" {
		err = errors.Warning("rbac: build failed").WithCause(errors.Warning("user table is required")).WithMeta("store", s.Name())
		return
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
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	row, queryErr := dal.QueryOne[*RoleRow](ctx, dal.NewConditions(dal.Eq("ID", param.Id)))
	if queryErr != nil {
		err = errors.Warning("rbac: save failed").WithCause(queryErr).WithMeta("store", s.Name()).WithMeta("id", param.Id)
		return
	}
	policies := make([]*Policy, 0, 1)
	if param.Policies != nil {
		for _, policy := range param.Policies {
			if policy == nil {
				continue
			}
			if policy.Object == "" {
				err = errors.Warning("rbac: save failed").WithCause(errors.Warning("object of policy is required")).WithMeta("store", s.Name()).WithMeta("id", param.Id)
				return
			}
			policies = append(policies, &Policy{
				Object: policy.Object,
				Action: policy.Action,
			})
		}
	}
	if row == nil {
		row = &RoleRow{
			Id:          param.Id,
			CreateBY:    "",
			CreateAT:    time.Time{},
			ModifyBY:    "",
			ModifyAT:    time.Time{},
			Version:     1,
			Name:        param.Name,
			Description: param.Description,
			ParentId:    param.ParentId,
			Children:    nil,
			Policies:    policies,
		}
		err = dal.Insert[*RoleRow](ctx, row)
		if err != nil {
			err = errors.Warning("rbac: save failed").WithCause(err).WithMeta("store", s.Name()).WithMeta("id", param.Id)
			return
		}
	} else {
		row = &RoleRow{
			Id:          row.Id,
			CreateBY:    row.CreateBY,
			CreateAT:    row.CreateAT,
			ModifyBY:    row.ModifyBY,
			ModifyAT:    row.ModifyAT,
			Version:     row.Version,
			Name:        param.Name,
			Description: param.Description,
			ParentId:    param.ParentId,
			Children:    nil,
			Policies:    policies,
		}
		err = dal.Update[*RoleRow](ctx, row)
		if err != nil {
			err = errors.Warning("rbac: save failed").WithCause(err).WithMeta("store", s.Name()).WithMeta("id", param.Id)
			return
		}
	}
	return
}

func (s *store) Remove(ctx context.Context, roleId string) (err errors.CodeError) {
	if roleId == "" {
		err = errors.Warning("rbac: remove failed").WithCause(errors.Warning("roleId is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	exits, existErr := dal.Exist[*RoleRow](ctx, dal.NewConditions(dal.Eq("PARENT_ID", roleId)))
	if existErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(existErr).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	if exits {
		err = errors.Warning("rbac: remove failed").WithCause(rbac.ErrCantRemoveHasChildrenRow).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	row, queryErr := dal.QueryOne[*RoleRow](ctx, dal.NewConditions(dal.Eq("ID", roleId)))
	if queryErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(queryErr).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	if row == nil {
		return
	}
	rmRoleErr := dal.Delete(ctx, row)
	if rmRoleErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(rmRoleErr).WithMeta("store", s.Name()).WithMeta("id", roleId)
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
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	row, queryErr := dal.QueryTree[*RoleRow, string](ctx, nil, nil, nil, roleId)
	if queryErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(queryErr).WithMeta("store", s.Name()).WithMeta("id", roleId)
		return
	}
	if row == nil {
		err = errors.Warning("rbac: get failed").WithCause(rbac.ErrRoleNofFound).WithMeta("id", roleId).WithMeta("store", s.Name())
		return
	}
	cpErr := copier.Copy(&role, role)
	if cpErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(cpErr).WithMeta("id", roleId).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) List(ctx context.Context, roleIds []string) (roles []*rbac.Role, err errors.CodeError) {
	if s.database != "" {
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	var rows []*RoleRow
	if roleIds == nil || len(roleIds) == 0 {
		rows, err = dal.QueryRootTrees[*RoleRow, string](ctx, nil, nil, nil)
	} else {
		rows, err = dal.QueryTrees[*RoleRow, string](ctx, nil, nil, nil, roleIds...)
	}
	if err != nil {
		err = errors.Warning("rbac: list failed").WithCause(err).WithMeta("store", s.Name()).WithMeta("roleIds", strings.Join(roleIds, ", "))
		return
	}
	if rows == nil || len(rows) == 0 {
		return
	}
	roles = make([]*rbac.Role, 0, 1)
	cpErr := copier.Copy(&roles, rows)
	if cpErr != nil {
		err = errors.Warning("rbac: list failed").WithCause(cpErr).WithMeta("roleIds", strings.Join(roleIds, ", ")).WithMeta("store", s.Name())
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
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	if param.RoleIds == nil || len(param.RoleIds) == 0 {
		// remove all
		err = dal.Delete(ctx, &UserRoleRow{
			Id:      param.UserId,
			RoleIds: nil,
		})
		if err != nil {
			if param.RoleIds == nil {
				param.RoleIds = []string{}
			}
			err = errors.Warning("rbac: bind failed").WithCause(err).
				WithMeta("userId", param.UserId).WithMeta("roleIds", strings.Join(param.RoleIds, ", ")).
				WithMeta("store", s.Name())
			return
		}
		return
	}
	row, queryErr := dal.QueryOne[*UserRoleRow](ctx, dal.NewConditions(dal.Eq("ID", param.UserId)))
	if queryErr != nil {
		err = errors.Warning("rbac: bind failed").WithCause(queryErr).
			WithMeta("userId", param.UserId).WithMeta("roleIds", strings.Join(param.RoleIds, ", ")).
			WithMeta("store", s.Name())
		return
	}
	sort.Strings(param.RoleIds)
	if row == nil {
		row = &UserRoleRow{
			Id:      param.UserId,
			RoleIds: param.RoleIds,
			Version: 1,
		}
		err = dal.Insert(ctx, row)
		if err != nil {
			err = errors.Warning("rbac: bind failed").WithCause(err).WithMeta("userId", param.UserId).WithMeta("roleIds", strings.Join(param.RoleIds, ", ")).WithMeta("store", s.Name())
			return
		}
		return
	}
	row.RoleIds = param.RoleIds
	err = dal.Update(ctx, row)
	if err != nil {
		err = errors.Warning("rbac: bind failed").WithCause(err).WithMeta("userId", param.UserId).WithMeta("roleIds", strings.Join(param.RoleIds, ", ")).WithMeta("store", s.Name())
		return
	}
	return
}

func (s *store) Bounds(ctx context.Context, userId string) (roles []*rbac.Role, err errors.CodeError) {
	if userId == "" {
		err = errors.Warning("rbac: bounds failed").WithCause(errors.Warning("user id is required")).WithMeta("store", s.Name())
		return
	}
	if s.database != "" {
		ctx = db.WithOptions(ctx, db.Database(s.database))
	}
	row, rowErr := dal.QueryOne[*UserRoleRow](ctx, dal.NewConditions(dal.Eq("ID", userId)))
	if rowErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(rowErr).WithMeta("userId", userId).WithMeta("store", s.Name())
		return
	}
	if row == nil {
		return
	}
	if row.RoleIds == nil || len(row.RoleIds) == 0 {
		return
	}
	rows, rowsErr := dal.QueryTrees[*RoleRow, string](ctx, nil, nil, nil, row.RoleIds...)
	if rowsErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(rowsErr).WithMeta("userId", userId).WithMeta("store", s.Name())
		return
	}
	if rows == nil || len(rows) == 0 {
		row.RoleIds = []string{}
		modErr := dal.Update(ctx, row)
		if modErr != nil {
			if s.log.DebugEnabled() {
				s.log.Debug().Cause(modErr).With("rbac", s.Name()).With("userId", userId).Message("rbac: clean user roles failed")
			}
		}
		return
	}
	if len(rows) < len(row.RoleIds) {
		roleIds := make([]string, 0, 1)
		rowsLen := len(rows)
		for _, id := range row.RoleIds {
			pos := sort.Search(rowsLen, func(i int) bool {
				return rows[i].Id == id
			})
			if pos < rowsLen {
				roleIds = append(roleIds, id)
			}
		}
		row.RoleIds = roleIds
		_ = dal.Update(ctx, row)
	}
	roles = make([]*rbac.Role, 0, 1)
	cpErr := copier.Copy(&roles, rows)
	if cpErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(cpErr).WithMeta("userId", userId).WithMeta("store", s.Name())
		return
	}
	return
}
