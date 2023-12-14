package sql

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
)

var (
	roleSchemaName = ""
	roleTableName  = ""
	userSchemaName = ""
	userTableName  = ""
)

type Policy struct {
	Object string `json:"object"`
	Action string `json:"action"`
}

type Role struct {
	Id          string           `column:"ID,pk" json:"id" tree:"ParentId+Children"`
	CreateBY    string           `column:"CREATE_BY,acb" json:"createBY"`
	CreateAT    sql.NullDatetime `column:"CREATE_AT,act" json:"createAT"`
	ModifyBY    string           `column:"MODIFY_BY,amb" json:"modifyBY"`
	ModifyAT    sql.NullDatetime `column:"MODIFY_AT,amt" json:"modifyAT"`
	Version     int64            `column:"VERSION,aol" json:"version"`
	Name        string           `column:"NAME" json:"name"`
	Description string           `column:"DESCRIPTION" json:"description"`
	ParentId    string           `column:"PARENT_ID" json:"parentId"`
	Policies    []Policy         `column:"POLICIES,json" json:"policies"`
}

func (row Role) TableInfo() dac.TableInfo {
	return dac.Info(roleTableName, dac.Schema(roleSchemaName))
}

type UserRole struct {
	Id      string   `column:"ID,pk" json:"id"`
	RoleIds []string `column:"ROLE_IDS,json" json:"roleIds"`
	Version int64    `column:"VERSION,aol" json:"version"`
}

func (row UserRole) TableInfo() dac.TableInfo {
	return dac.Info(userTableName, dac.Schema(userSchemaName))
}
