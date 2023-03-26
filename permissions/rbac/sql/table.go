package sql

import (
	"time"
)

var (
	_roleSchema = ""
	_roleTable  = ""
	_userSchema = ""
	_userTable  = ""
)

type Policy struct {
	Object string `json:"object"`
	Action string `json:"action"`
}

type RoleRows []*RoleRow

func (rows RoleRows) Len() int {
	return len(rows)
}

func (rows RoleRows) Less(i, j int) bool {
	return rows[i].Id < rows[j].Id
}

func (rows RoleRows) Swap(i, j int) {
	rows[i], rows[j] = rows[j], rows[i]
	return
}

type RoleRow struct {
	Id          string    `col:"ID,pk" json:"id"`
	CreateBY    string    `col:"CREATE_BY,acb" json:"createBY"`
	CreateAT    time.Time `col:"CREATE_AT,act" json:"createAT"`
	ModifyBY    string    `col:"MODIFY_BY,amb" json:"modifyBY"`
	ModifyAT    time.Time `col:"MODIFY_AT,amt" json:"modifyAT"`
	Version     int64     `col:"VERSION,aol" json:"version"`
	Name        string    `col:"NAME" json:"name"`
	Description string    `col:"DESCRIPTION" json:"description"`
	ParentId    string    `col:"PARENT_ID" json:"parentId"`
	Children    RoleRows  `col:"CHILDREN,tree,ID+PARENT_ID" json:"children"`
	Policies    []*Policy `col:"POLICIES,json" json:"policies"`
}

func (t *RoleRow) TableName() (schema string, table string) {
	schema = _roleSchema
	table = _roleTable
	return
}

type UserRoleRow struct {
	Id      string   `col:"ID,pk" json:"id"`
	RoleIds []string `col:"ROLE_IDS,json" json:"roleIds"`
	Version int64    `col:"VERSION,aol" json:"version"`
}

func (t *UserRoleRow) TableName() (schema string, table string) {
	schema = _userSchema
	table = _userTable
	return
}
