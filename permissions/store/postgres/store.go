package postgres

import (
	"container/list"
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/postgres"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/permissions"
	"github.com/aacfactory/logs"
	"strings"
)

type TableName struct {
	Schema string
	Table  string
}

type Config struct {
	Model  TableName
	Policy TableName
}

func Component() (component service.Component) {
	component = &store{}
	return
}

type store struct {
	log    logs.Logger
	Model  TableName
	Policy TableName
}

func (st *store) Name() string {
	return "store"
}

func (st *store) Build(options service.ComponentOptions) (err error) {
	st.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("permissions postgres store: build failed, decode config failed").WithCause(configErr)
		return
	}
	modelSchema := strings.TrimSpace(config.Model.Schema)
	modelTable := strings.TrimSpace(config.Model.Table)
	if modelTable == "" {
		err = errors.Warning("permissions postgres store: build failed, model table in config is required")
		return
	}
	st.Model.Schema = modelSchema
	st.Model.Table = modelTable
	policySchema := strings.TrimSpace(config.Policy.Schema)
	policyTable := strings.TrimSpace(config.Policy.Table)
	if modelTable == "" {
		err = errors.Warning("permissions postgres store: build failed, policy table in config is required")
		return
	}
	st.Policy.Schema = policySchema
	st.Policy.Table = policyTable
	return
}

func (st *store) Role(ctx context.Context, name string) (role *permissions.Role, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		err = errors.BadRequest("permissions postgres store: get role failed, name is empty")
		return
	}
	roles, rolesErr := st.Roles(ctx)
	if rolesErr != nil {
		err = errors.BadRequest("permissions postgres store: get role failed").WithCause(rolesErr)
		return
	}
	if roles == nil || len(roles) == 0 {
		err = errors.BadRequest("permissions postgres store: get role failed").WithCause(fmt.Errorf("%s was not exist", name))
		return
	}
	target, has := permissions.FindRole(roles, name)
	if !has {
		err = errors.BadRequest("permissions postgres store: get role failed").WithCause(fmt.Errorf("%s was not exist", name))
		return
	}
	role = target
	return
}

func (st *store) Roles(ctx context.Context) (roles []*permissions.Role, err error) {
	query := `SELECT "NAME", "PARENT", "RESOURCES", "VERSION" FROM `
	if st.Model.Schema != "" {
		query = query + `"` + st.Model.Schema + `".`
	}
	query = query + `"` + st.Model.Table + `"`
	rows, queryErr := postgres.QueryContext(ctx, query)
	if queryErr != nil {
		err = errors.ServiceError("permissions postgres store: get roles failed").WithCause(queryErr)
		return
	}
	if rows.Empty() {
		return
	}
	models := list.New()
	for {
		row, has := rows.Next()
		if !has {
			break
		}
		model := &ModelRow{}
		scanErr := row.Scan(ctx, model)
		if scanErr != nil {
			err = errors.ServiceError("permissions postgres store: get roles failed").WithCause(scanErr)
			return
		}
		models.PushBack(model)
	}
	roles = modelsMapToRoles(models, "")
	return
}

func (st *store) SaveRole(ctx context.Context, role *permissions.Role) (err error) {
	row := &ModelRow{
		schema: st.Model.Schema,
		table:  st.Model.Table,
	}
	fetched, queryErr := postgres.QueryOne(ctx, postgres.NewConditions(postgres.Eq("NAME", role.Name)), row)
	if queryErr != nil {
		err = errors.ServiceError("permissions postgres store: save role failed").WithCause(queryErr)
		return
	}
	row.Parent = role.Parent
	row.Resources = role.Resources
	if fetched {
		row.Name = role.Name
		modErr := postgres.Modify(ctx, row)
		if modErr != nil {
			err = errors.ServiceError("permissions postgres store: save role failed").WithCause(modErr)
			return
		}
	} else {
		insertErr := postgres.Insert(ctx, row)
		if insertErr != nil {
			err = errors.ServiceError("permissions postgres store: save role failed").WithCause(insertErr)
			return
		}
	}
	return
}

func (st *store) RemoveRole(ctx context.Context, name string) (err error) {
	row := &ModelRow{
		schema: st.Model.Schema,
		table:  st.Model.Table,
	}
	fetched, queryErr := postgres.QueryOne(ctx, postgres.NewConditions(postgres.Eq("NAME", name)), row)
	if queryErr != nil {
		err = errors.ServiceError("permissions postgres store: remove role failed").WithCause(queryErr)
		return
	}
	if !fetched {
		return
	}
	rmErr := postgres.Delete(ctx, row)
	if rmErr != nil {
		err = errors.ServiceError("permissions postgres store: remove role failed").WithCause(rmErr)
		return
	}
	return
}

func (st *store) UserRoles(ctx context.Context, userId string) (roles []*permissions.Role, err error) {
	policy := &PolicyRow{
		schema: st.Policy.Schema,
		table:  st.Policy.Table,
	}
	fetched, queryErr := postgres.QueryOne(ctx, postgres.NewConditions(postgres.Eq("USER_ID", userId)), policy)
	if queryErr != nil {
		err = errors.ServiceError("permissions postgres store: get user roles failed").WithCause(queryErr)
		return
	}
	if !fetched {
		return
	}
	if policy.Roles == nil || len(policy.Roles) == 0 {
		return
	}
	allRoles, rolesErr := st.Roles(ctx)
	if rolesErr != nil {
		err = errors.BadRequest("permissions postgres store: get user roles failed").WithCause(rolesErr)
		return
	}
	if allRoles == nil || len(allRoles) == 0 {
		err = errors.BadRequest("permissions postgres store: get user roles failed").WithCause(fmt.Errorf("there is no roles in system"))
		return
	}
	roles = make([]*permissions.Role, 0, 1)
	for _, role := range policy.Roles {
		target, has := permissions.FindRole(allRoles, role)
		if has {
			roles = append(roles, target)
		}
	}
	return
}

func (st *store) UserBindRoles(ctx context.Context, userId string, roleNames ...string) (err error) {
	policy := &PolicyRow{
		schema: st.Policy.Schema,
		table:  st.Policy.Table,
	}
	fetched, queryErr := postgres.QueryOne(ctx, postgres.NewConditions(postgres.Eq("USER_ID", userId)), policy)
	if queryErr != nil {
		err = errors.ServiceError("permissions postgres store: user bind roles failed").WithCause(queryErr)
		return
	}
	if !fetched {
		policy.UserId = userId
	}
	if policy.Roles == nil {
		policy.Roles = make([]string, 0, 1)
	}
	binds := 0
	for _, name := range roleNames {
		bind := false
		for _, role := range policy.Roles {
			if role == name {
				bind = true
				break
			}
		}
		if bind {
			continue
		}
		policy.Roles = append(policy.Roles, name)
		binds++
	}
	if binds > 0 {
		if fetched {
			modeErr := postgres.Modify(ctx, policy)
			if modeErr != nil {
				err = errors.ServiceError("permissions postgres store: user bind roles failed").WithCause(modeErr)
				return
			}
		} else {
			insertErr := postgres.Insert(ctx, policy)
			if insertErr != nil {
				err = errors.ServiceError("permissions postgres store: user bind roles failed").WithCause(insertErr)
				return
			}
		}
	}
	return
}

func (st *store) UserUnbindRoles(ctx context.Context, userId string, roleNames ...string) (err error) {
	policy := &PolicyRow{
		schema: st.Policy.Schema,
		table:  st.Policy.Table,
	}
	fetched, queryErr := postgres.QueryOne(ctx, postgres.NewConditions(postgres.Eq("USER_ID", userId)), policy)
	if queryErr != nil {
		err = errors.ServiceError("permissions postgres store: user unbind roles failed").WithCause(queryErr)
		return
	}
	if !fetched {
		return
	}
	remains := make([]string, 0, 1)
	for _, role := range policy.Roles {
		unbind := false
		for _, name := range roleNames {
			if role == name {
				unbind = true
				break
			}
		}
		if unbind {
			continue
		}
		remains = append(remains, role)
	}
	if len(remains) == len(policy.Roles) {
		return
	}
	policy.Roles = remains
	modeErr := postgres.Modify(ctx, policy)
	if modeErr != nil {
		err = errors.ServiceError("permissions postgres store: user unbind roles failed").WithCause(modeErr)
		return
	}
	return
}

func (st *store) Close() {
	return
}
