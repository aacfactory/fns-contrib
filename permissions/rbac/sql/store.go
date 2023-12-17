package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/permissions/rbac"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/container/trees"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"strings"
	"time"
)

func Store() rbac.Store {
	return &store{}
}

type store struct {
	log                     logs.Logger
	endpoint                []byte
	enableCache             bool
	rolesCacheKey           []byte
	rolesCacheTTL           time.Duration
	userRolesCacheKeyPrefix []byte
	userRolesCacheTTL       time.Duration
	rolesLockerName         []byte
	rolesBarrierName        []byte
}

func (s *store) Name() (name string) {
	name = "sql"
	return
}

func (s *store) Construct(options services.Options) (err error) {
	s.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("rbac: construct sql store failed").WithCause(configErr)
		return
	}
	s.endpoint = bytex.FromString(strings.TrimSpace(config.Endpoint))
	roleSchemaName = config.RoleTable.Schema
	roleTableName = config.RoleTable.Table
	userSchemaName = config.UserTable.Schema
	userTableName = config.UserTable.Table
	if roleTableName == "" || userTableName == "" {
		err = errors.Warning("rbac: construct sql store failed").WithCause(fmt.Errorf("table name is required"))
		return
	}
	s.enableCache = !config.Cache.Disable
	if s.enableCache {
		s.rolesCacheKey = []byte("fns:permissions:rbac:roles")
		s.userRolesCacheKeyPrefix = []byte("fns:permissions:rbac:uses:")
		s.rolesCacheTTL = config.Cache.RolesTTL
		if s.rolesCacheTTL < 1 {
			s.rolesCacheTTL = 24 * time.Hour
		}
		s.userRolesCacheTTL = config.Cache.UserRolesTTL
		if s.userRolesCacheTTL < 1 {
			s.userRolesCacheTTL = 12 * time.Hour
		}
	}
	s.rolesLockerName = []byte("fns:permissions:rbac:roles")
	s.rolesBarrierName = []byte("fns:permissions:rbac:roles")
	return
}

func (s *store) Shutdown(_ context.Context) {
	return
}

func (s *store) Role(ctx context.Context, id string) (role rbac.Role, has bool, err error) {
	roles, rolesErr := s.Roles(ctx)
	if rolesErr != nil {
		err = rolesErr
		return
	}
	role, has = roles.Get(id)
	return
}

func (s *store) Roles(ctx context.Context) (roles rbac.Roles, err error) {
	if s.enableCache {
		// cache
		sc := runtime.SharedStore(ctx)
		p, has, getErr := sc.Get(ctx, s.rolesCacheKey)
		if getErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: get roles from cache failed").WithCause(getErr)).
					With("action", "cache").Message("permissions: get roles from cache failed")
			}
		}
		if has {
			roles = make(rbac.Roles, 0, 1)
			decodeErr := json.Unmarshal(p, &roles)
			if decodeErr != nil {
				if s.log.WarnEnabled() {
					s.log.Warn().Cause(errors.Warning("permissions: decode roles failed").WithCause(getErr)).
						With("action", "decode").Message("permissions: decode roles failed")
				}
			} else {
				return
			}
		}
	}

	r, doErr := runtime.Barrier(ctx, s.rolesBarrierName, func() (result interface{}, err error) {
		if len(s.endpoint) > 0 {
			dac.Use(ctx, s.endpoint)
		}
		rows, queryErr := dac.ALL[Role](ctx)
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		if queryErr != nil {
			err = errors.Warning("permissions: get roles failed").WithCause(queryErr)
			return
		}
		roles = make(rbac.Roles, 0, 1)
		for _, row := range rows {
			policies := make([]rbac.Policy, 0, 1)
			for _, policy := range row.Policies {
				policies = append(policies, rbac.Policy{
					Object: policy.Object,
					Action: policy.Action,
				})
			}
			roles = append(roles, rbac.Role{
				Id:          row.Id,
				Name:        row.Name,
				Description: row.Description,
				ParentId:    row.ParentId,
				Children:    nil,
				Policies:    policies,
			})
		}
		roles, err = trees.ConvertListToTree[rbac.Role](roles)
		if err != nil {
			err = errors.Warning("permissions: get roles failed").WithCause(err)
			return
		}
		result = roles
		return
	})
	if doErr != nil {
		err = doErr
		return
	}
	roles = make(rbac.Roles, 0, 1)
	err = r.Unmarshal(&roles)
	if err != nil {
		err = errors.Warning("permissions: get roles failed").WithCause(err)
		return
	}

	if s.enableCache && len(roles) > 0 {
		// cache
		p, encodeErr := json.Marshal(roles)
		if encodeErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: encode roles failed").WithCause(encodeErr)).
					With("action", "encode").Message("permissions: encode roles failed")
			}
			return
		}
		sc := runtime.SharedStore(ctx)
		setErr := sc.SetWithTTL(ctx, s.rolesCacheKey, p, s.rolesCacheTTL)
		if setErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: cache roles failed").WithCause(setErr)).
					With("action", "cache").Message("permissions: cache roles failed")
			}
			return
		}
	}
	return
}

func (s *store) SaveRole(ctx context.Context, role rbac.Role) (err error) {
	locker, lockerErr := runtime.AcquireLocker(ctx, s.rolesLockerName, 10*time.Second)
	if lockerErr != nil {
		err = errors.Warning("permissions: save role failed").WithCause(lockerErr)
		return
	}
	lockErr := locker.Lock(ctx)
	if lockErr != nil {
		err = errors.Warning("permissions: save role failed").WithCause(lockErr)
		return
	}

	if len(s.endpoint) > 0 {
		dac.Use(ctx, s.endpoint)
	}
	useTx := false
	if len(role.Children) > 0 {
		beginErr := dac.Begin(ctx)
		if beginErr != nil {
			if len(s.endpoint) > 0 {
				dac.Disuse(ctx)
			}
			err = errors.Warning("permissions: save role failed").WithCause(beginErr)
			_ = locker.Unlock(ctx)
			return
		}
		useTx = true
	}
	err = s.saveRole(ctx, role)
	if err != nil {
		if useTx {
			dac.Rollback(ctx)
		}
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		err = errors.Warning("permissions: save role failed").WithCause(err)
		_ = locker.Unlock(ctx)
		return
	}
	if useTx {
		cmtErr := dac.Commit(ctx)
		if cmtErr != nil {
			if len(s.endpoint) > 0 {
				dac.Disuse(ctx)
			}
			err = errors.Warning("permissions: save role failed").WithCause(cmtErr)
			_ = locker.Unlock(ctx)
			return
		}
	}
	if len(s.endpoint) > 0 {
		dac.Disuse(ctx)
	}
	// cache
	if s.enableCache {
		sc := runtime.SharedStore(ctx)
		rmErr := sc.Remove(ctx, s.rolesCacheKey)
		if rmErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: clean roles cache failed").WithCause(rmErr)).
					With("action", "cache").Message("permissions: clean roles cache failed")
			}
		}
		_, _ = s.Roles(ctx)
	}
	_ = locker.Unlock(ctx)
	return
}

func (s *store) saveRole(ctx context.Context, role rbac.Role) (err error) {
	row, has, getErr := dac.One[Role](ctx, dac.Conditions(dac.Eq("Name", role.Name)))
	if getErr != nil {
		err = getErr
		return
	}
	if has {
		row.Name = role.Name
		row.Description = role.Description
		row.ParentId = role.ParentId
		policies := make([]Policy, 0, 1)
		for _, policy := range role.Policies {
			policies = append(policies, Policy{
				Object: policy.Object,
				Action: policy.Action,
			})
		}
		row.Policies = policies
		updated := false
		row, updated, err = dac.Update[Role](ctx, row)
		if err != nil {
			return
		}
		if !updated {
			err = errors.Warning("no affected")
			return
		}
	} else {
		policies := make([]Policy, 0, 1)
		for _, policy := range role.Policies {
			policies = append(policies, Policy{
				Object: policy.Object,
				Action: policy.Action,
			})
		}
		id := role.Id
		if id == "" {
			id = uid.UID()
		}
		row = Role{
			Id:          id,
			CreateBY:    "",
			CreateAT:    sql.NullDatetime{},
			ModifyBY:    "",
			ModifyAT:    sql.NullDatetime{},
			Version:     0,
			Name:        role.Name,
			Description: role.Id,
			ParentId:    role.ParentId,
			Policies:    policies,
		}
		inserted := false
		row, inserted, err = dac.Insert[Role](ctx, row)
		if err != nil {
			return
		}
		if !inserted {
			err = errors.Warning("no affected")
			return
		}
	}
	for _, child := range role.Children {
		err = s.saveRole(ctx, child)
		if err != nil {
			return
		}
	}
	return
}

func (s *store) RemoveRole(ctx context.Context, role rbac.Role) (err error) {
	ids := role.Ids()
	if len(ids) == 0 {
		err = errors.Warning("permissions: remove role failed").WithCause(fmt.Errorf("no role to be removed"))
		return
	}

	locker, lockerErr := runtime.AcquireLocker(ctx, s.rolesLockerName, 10*time.Second)
	if lockerErr != nil {
		err = errors.Warning("permissions: remove role failed").WithCause(lockerErr)
		return
	}
	lockErr := locker.Lock(ctx)
	if lockErr != nil {
		err = errors.Warning("permissions: remove role failed").WithCause(lockErr)
		return
	}

	if len(s.endpoint) > 0 {
		dac.Use(ctx, s.endpoint)
	}
	_, rmErr := dac.DeleteByCondition[Role](ctx, dac.In("Id", ids))
	if len(s.endpoint) > 0 {
		dac.Disuse(ctx)
	}
	if rmErr != nil {
		err = errors.Warning("permissions: remove role failed").WithCause(rmErr)
		_ = locker.Unlock(ctx)
		return
	}

	// cache
	if s.enableCache {
		sc := runtime.SharedStore(ctx)
		p, has, getErr := sc.Get(ctx, s.rolesCacheKey)
		if getErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: remove roles from cache failed").WithCause(getErr)).
					With("action", "cache").Message("permissions: remove roles from cache failed")
			}
		}
		if has {
			roles := make(rbac.Roles, 0, 1)
			decodeErr := json.Unmarshal(p, &roles)
			if decodeErr != nil {
				if s.log.WarnEnabled() {
					s.log.Warn().Cause(errors.Warning("permissions: decode roles failed").WithCause(getErr)).
						With("action", "decode").Message("permissions: decode roles failed")
				}
				_ = locker.Unlock(ctx)
				return
			}
			roles = roles.Remove(role)
			b, encodeErr := json.Marshal(roles)
			if encodeErr != nil {
				if s.log.WarnEnabled() {
					s.log.Warn().Cause(errors.Warning("permissions: decode roles failed").WithCause(getErr)).
						With("action", "encode").Message("permissions: decode roles failed")
				}
				_ = locker.Unlock(ctx)
				return
			}
			setErr := sc.SetWithTTL(ctx, s.rolesCacheKey, b, s.rolesCacheTTL)
			if setErr != nil {
				if s.log.WarnEnabled() {
					s.log.Warn().Cause(errors.Warning("permissions: remove roles from cache failed").WithCause(setErr)).
						With("action", "cache").Message("permissions: remove roles from cache failed")
				}
				_ = locker.Unlock(ctx)
				return
			}
		}
	}
	_ = locker.Unlock(ctx)
	return
}

func (s *store) Bind(ctx context.Context, account authorizations.Id, roles []rbac.Role) (err error) {
	if !account.Exist() {
		err = errors.Warning("permissions: bind role failed").WithCause(fmt.Errorf("account is required"))
		return
	}
	if len(roles) == 0 {
		err = errors.Warning("permissions: bind role failed").WithCause(fmt.Errorf("roles are required"))
		return
	}
	userId := account.String()
	ids := make([]string, 0, 1)
	for _, role := range roles {
		ids = append(ids, role.Id)
	}

	if len(s.endpoint) > 0 {
		dac.Use(ctx, s.endpoint)
	}

	row, has, getErr := dac.One[UserRole](ctx, dac.Conditions(dac.Eq("Id", userId)))
	if getErr != nil {
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		err = errors.Warning("permissions: bind role failed").WithCause(getErr)
		return
	}
	if has {
		row.RoleIds = append(row.RoleIds, ids...)
		updated := false
		row, updated, err = dac.Update[UserRole](ctx, row)
		if err != nil {
			if len(s.endpoint) > 0 {
				dac.Disuse(ctx)
			}
			err = errors.Warning("permissions: bind role failed").WithCause(err)
			return
		}
		if !updated {
			if len(s.endpoint) > 0 {
				dac.Disuse(ctx)
			}
			err = errors.Warning("permissions: bind role failed").WithCause(fmt.Errorf("no affected"))
			return
		}
	} else {
		row = UserRole{
			Id:      userId,
			RoleIds: ids,
			Version: 0,
		}
		inserted := false
		row, inserted, err = dac.Insert[UserRole](ctx, row)
		if err != nil {
			if len(s.endpoint) > 0 {
				dac.Disuse(ctx)
			}
			err = errors.Warning("permissions: bind role failed").WithCause(err)
			return
		}
		if !inserted {
			if len(s.endpoint) > 0 {
				dac.Disuse(ctx)
			}
			err = errors.Warning("permissions: bind role failed").WithCause(fmt.Errorf("no affected"))
			return
		}
	}
	// cache
	if s.enableCache {
		p, encodeErr := json.Marshal(row)
		if encodeErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: bind role failed").WithCause(encodeErr)).
					With("action", "encode").Message("permissions: bind role failed")
			}
			return
		}
		key := append(s.userRolesCacheKeyPrefix, userId...)
		sc := runtime.SharedStore(ctx)
		setErr := sc.SetWithTTL(ctx, key, p, s.userRolesCacheTTL)
		if setErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: bind role failed").WithCause(setErr)).
					With("action", "cache").Message("permissions: bind role failed")
			}
			return
		}
	}

	return
}

func (s *store) Unbind(ctx context.Context, account authorizations.Id, roles []rbac.Role) (err error) {
	if !account.Exist() {
		err = errors.Warning("permissions: unbind role failed").WithCause(fmt.Errorf("account is required"))
		return
	}
	if len(roles) == 0 {
		err = errors.Warning("permissions: unbind role failed").WithCause(fmt.Errorf("roles are required"))
		return
	}
	userId := account.String()
	ids := make([]string, 0, 1)
	for _, role := range roles {
		ids = append(ids, role.Id)
	}
	if len(s.endpoint) > 0 {
		dac.Use(ctx, s.endpoint)
	}
	row, has, getErr := dac.One[UserRole](ctx, dac.Conditions(dac.Eq("Id", userId)))
	if getErr != nil {
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		err = errors.Warning("permissions: unbind role failed").WithCause(getErr)
		return
	}
	if !has {
		err = errors.Warning("permissions: unbind role failed").WithCause(fmt.Errorf("has no bounds"))
		return
	}
	newIds := make([]string, 0, 1)
	for _, id := range row.RoleIds {
		in := false
		for _, eid := range ids {
			if id == eid {
				in = true
				break
			}
		}
		if in {
			continue
		}
		newIds = append(newIds, id)
	}
	row.RoleIds = newIds
	updated := false
	row, updated, err = dac.Update[UserRole](ctx, row)
	if err != nil {
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		err = errors.Warning("permissions: unbind role failed").WithCause(err)
		return
	}
	if !updated {
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		err = errors.Warning("permissions: unbind role failed").WithCause(fmt.Errorf("no affected"))
		return
	}

	// cache
	if s.enableCache {
		p, encodeErr := json.Marshal(row)
		if encodeErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: unbind role failed").WithCause(encodeErr)).
					With("action", "encode").Message("permissions: unbind role failed")
			}
			return
		}
		key := append(s.userRolesCacheKeyPrefix, userId...)
		sc := runtime.SharedStore(ctx)
		setErr := sc.SetWithTTL(ctx, key, p, s.userRolesCacheTTL)
		if setErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: unbind role failed").WithCause(setErr)).
					With("action", "cache").Message("permissions: unbind role failed")
			}
			return
		}
	}
	return
}

func (s *store) Bounds(ctx context.Context, account authorizations.Id) (roles rbac.Roles, err error) {
	if !account.Exist() {
		err = errors.Warning("permissions: bounds role failed").WithCause(fmt.Errorf("account is required"))
		return
	}
	userId := account.String()
	row := UserRole{}
	cacheHit := false
	if s.enableCache {
		key := append(s.userRolesCacheKeyPrefix, userId...)
		sc := runtime.SharedStore(ctx)
		p, has, getErr := sc.Get(ctx, key)
		if getErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: bounds role failed").WithCause(getErr)).
					With("action", "cache").Message("permissions: bounds role failed")
			}
		}
		if has {
			decodeErr := json.Unmarshal(p, &row)
			if decodeErr != nil {
				if s.log.WarnEnabled() {
					s.log.Warn().Cause(errors.Warning("permissions: bounds role failed").WithCause(decodeErr)).
						With("action", "decode").Message("permissions: bounds role failed")
				}
			} else {
				cacheHit = true
			}
		}
	}
	if row.Id == "" {
		if len(s.endpoint) > 0 {
			dac.Use(ctx, s.endpoint)
		}
		has := false
		row, has, err = dac.One[UserRole](ctx, dac.Conditions(dac.Eq("Id", userId)))
		if len(s.endpoint) > 0 {
			dac.Disuse(ctx)
		}
		if err != nil {
			err = errors.Warning("permissions: bounds role failed").WithCause(err)
			return
		}
		if !has {
			return
		}
	}
	if len(row.RoleIds) == 0 {
		return
	}
	all, allErr := s.Roles(ctx)
	if allErr != nil {
		err = errors.Warning("permissions: bounds role failed").WithCause(allErr)
		return
	}
	notFounds := make([]string, 0, 1)
	for _, roleId := range row.RoleIds {
		role, has := all.Get(roleId)
		if has {
			roles = append(roles, role)
			continue
		}
		notFounds = append(notFounds, roleId)
	}
	if len(notFounds) > 0 {
		if s.enableCache {
			key := append(s.userRolesCacheKeyPrefix, userId...)
			sc := runtime.SharedStore(ctx)
			_ = sc.Remove(ctx, key)
		}
		rows, getErr := dac.ALL[Role](ctx, dac.Conditions(dac.In("Id", notFounds)))
		if getErr != nil {
			err = errors.Warning("permissions: bounds role failed").WithCause(getErr)
			return
		}
		if len(rows) == 0 {
			return
		}
		extras := make(rbac.Roles, 0, 1)
		for _, role := range rows {
			policies := make([]rbac.Policy, 0, 1)
			for _, policy := range role.Policies {
				policies = append(policies, rbac.Policy{
					Object: policy.Object,
					Action: policy.Action,
				})
			}
			extras = append(extras, rbac.Role{
				Id:          role.Id,
				Name:        role.Name,
				Description: role.Description,
				ParentId:    role.ParentId,
				Children:    nil,
				Policies:    policies,
			})
		}
		extras, err = trees.ConvertListToTree(extras)
		if err != nil {
			err = errors.Warning("permissions: bounds role failed").WithCause(err)
			return
		}
		for _, extra := range extras {
			roles = roles.Add(extra)
		}
		cacheHit = false // refresh cache
	}
	if !cacheHit && s.enableCache {
		p, encodeErr := json.Marshal(row)
		if encodeErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: bounds role failed").WithCause(encodeErr)).
					With("action", "encode").Message("permissions: bounds role failed")
			}
			return
		}
		key := append(s.userRolesCacheKeyPrefix, userId...)
		sc := runtime.SharedStore(ctx)
		setErr := sc.SetWithTTL(ctx, key, p, s.userRolesCacheTTL)
		if setErr != nil {
			if s.log.WarnEnabled() {
				s.log.Warn().Cause(errors.Warning("permissions: bounds role failed").WithCause(setErr)).
					With("action", "cache").Message("permissions: bounds role failed")
			}
		}
	}
	return
}
