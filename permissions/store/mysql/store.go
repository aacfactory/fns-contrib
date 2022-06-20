package mysql

import (
	"container/list"
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/mysql"
	"github.com/aacfactory/fns/service/builtin/permissions"
	"github.com/aacfactory/logs"
	"strings"
)

func init() {
	permissions.RegisterStore(&Store{})
}

type TableName struct {
	Schema string
	Table  string
}

type Config struct {
	Model  TableName
	Policy TableName
}

type Store struct {
	log    logs.Logger
	Model  TableName
	Policy TableName
}

func (store *Store) Build(options permissions.StoreOptions) (err error) {
	store.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("permissions mysql store: build failed, decode config failed").WithCause(configErr)
		return
	}
	modelSchema := strings.TrimSpace(config.Model.Schema)
	modelTable := strings.TrimSpace(config.Model.Table)
	if modelTable == "" {
		err = errors.Warning("permissions mysql store: build failed, model table in config is required")
		return
	}
	store.Model.Schema = modelSchema
	store.Model.Table = modelTable
	policySchema := strings.TrimSpace(config.Policy.Schema)
	policyTable := strings.TrimSpace(config.Policy.Table)
	if modelTable == "" {
		err = errors.Warning("permissions mysql store: build failed, policy table in config is required")
		return
	}
	store.Policy.Schema = policySchema
	store.Policy.Table = policyTable
	return
}

func (store *Store) Role(ctx context.Context, name string) (role *permissions.Role, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		err = errors.BadRequest("permissions mysql store: get role failed, name is empty")
		return
	}
	roles, rolesErr := store.Roles(ctx)
	if rolesErr != nil {
		err = errors.BadRequest("permissions mysql store: get role failed").WithCause(rolesErr)
		return
	}
	if roles == nil || len(roles) == 0 {
		err = errors.BadRequest("permissions mysql store: get role failed").WithCause(fmt.Errorf("%s was not exist", name))
		return
	}
	target, has := permissions.FindRole(roles, name)
	if !has {
		err = errors.BadRequest("permissions mysql store: get role failed").WithCause(fmt.Errorf("%s was not exist", name))
		return
	}
	role = target
	return
}

func (store *Store) Roles(ctx context.Context) (roles []*permissions.Role, err error) {
	query := "SELECT `NAME`, `PARENT`, `RESOURCES`, `VERSION` FROM "
	if store.Model.Schema != "" {
		query = query + "`" + store.Model.Schema + "`."
	}
	query = query + "`" + store.Model.Table + "`"
	rows, queryErr := mysql.QueryContext(ctx, query)
	if queryErr != nil {
		err = errors.ServiceError("permissions mysql store: get roles failed").WithCause(queryErr)
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
			err = errors.ServiceError("permissions mysql store: get roles failed").WithCause(scanErr)
			return
		}
		models.PushBack(model)
	}
	roles = modelsMapToRoles(models, "")
	return
}

func (store *Store) SaveRole(ctx context.Context, role *permissions.Role) (err error) {
	row := &ModelRow{
		schema: store.Model.Schema,
		table:  store.Model.Table,
	}
	fetched, queryErr := mysql.QueryOne(ctx, mysql.NewConditions(mysql.Eq("NAME", role.Name)), row)
	if queryErr != nil {
		err = errors.ServiceError("permissions mysql store: save role failed").WithCause(queryErr)
		return
	}
	row.Parent = role.Parent
	row.Resources = role.Resources
	if fetched {
		row.Name = role.Name
		modErr := mysql.Modify(ctx, row)
		if modErr != nil {
			err = errors.ServiceError("permissions mysql store: save role failed").WithCause(modErr)
			return
		}
	} else {
		insertErr := mysql.Insert(ctx, row)
		if insertErr != nil {
			err = errors.ServiceError("permissions mysql store: save role failed").WithCause(insertErr)
			return
		}
	}
	return
}

func (store *Store) RemoveRole(ctx context.Context, name string) (err error) {
	row := &ModelRow{
		schema: store.Model.Schema,
		table:  store.Model.Table,
	}
	fetched, queryErr := mysql.QueryOne(ctx, mysql.NewConditions(mysql.Eq("NAME", name)), row)
	if queryErr != nil {
		err = errors.ServiceError("permissions mysql store: remove role failed").WithCause(queryErr)
		return
	}
	if !fetched {
		return
	}
	rmErr := mysql.Delete(ctx, row)
	if rmErr != nil {
		err = errors.ServiceError("permissions mysql store: remove role failed").WithCause(rmErr)
		return
	}
	return
}

func (store *Store) UserRoles(ctx context.Context, userId string) (roles []*permissions.Role, err error) {
	policy := &PolicyRow{
		schema: store.Policy.Schema,
		table:  store.Policy.Table,
	}
	fetched, queryErr := mysql.QueryOne(ctx, mysql.NewConditions(mysql.Eq("USER_ID", userId)), policy)
	if queryErr != nil {
		err = errors.ServiceError("permissions mysql store: get user roles failed").WithCause(queryErr)
		return
	}
	if !fetched {
		return
	}
	if policy.Roles == nil || len(policy.Roles) == 0 {
		return
	}
	allRoles, rolesErr := store.Roles(ctx)
	if rolesErr != nil {
		err = errors.BadRequest("permissions mysql store: get user roles failed").WithCause(rolesErr)
		return
	}
	if allRoles == nil || len(allRoles) == 0 {
		err = errors.BadRequest("permissions mysql store: get user roles failed").WithCause(fmt.Errorf("there is no roles in system"))
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

func (store *Store) UserBindRoles(ctx context.Context, userId string, roleNames ...string) (err error) {
	policy := &PolicyRow{
		schema: store.Policy.Schema,
		table:  store.Policy.Table,
	}
	fetched, queryErr := mysql.QueryOne(ctx, mysql.NewConditions(mysql.Eq("USER_ID", userId)), policy)
	if queryErr != nil {
		err = errors.ServiceError("permissions mysql store: user bind roles failed").WithCause(queryErr)
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
			modeErr := mysql.Modify(ctx, policy)
			if modeErr != nil {
				err = errors.ServiceError("permissions mysql store: user bind roles failed").WithCause(modeErr)
				return
			}
		} else {
			insertErr := mysql.Insert(ctx, policy)
			if insertErr != nil {
				err = errors.ServiceError("permissions mysql store: user bind roles failed").WithCause(insertErr)
				return
			}
		}
	}
	return
}

func (store *Store) UserUnbindRoles(ctx context.Context, userId string, roleNames ...string) (err error) {
	policy := &PolicyRow{
		schema: store.Policy.Schema,
		table:  store.Policy.Table,
	}
	fetched, queryErr := mysql.QueryOne(ctx, mysql.NewConditions(mysql.Eq("USER_ID", userId)), policy)
	if queryErr != nil {
		err = errors.ServiceError("permissions mysql store: user unbind roles failed").WithCause(queryErr)
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
	modeErr := mysql.Modify(ctx, policy)
	if modeErr != nil {
		err = errors.ServiceError("permissions mysql store: user unbind roles failed").WithCause(modeErr)
		return
	}
	return
}

func (store *Store) Close() (err error) {
	return
}
