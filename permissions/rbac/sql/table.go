package sql

import "github.com/aacfactory/fns-contrib/databases/sql/dal"

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

type RoleRow struct {
	dal.Audits
	Name        string     `col:"NAME" json:"name"`
	Description string     `col:"DESCRIPTION" json:"description"`
	ParentId    string     `col:"PARENT_ID" json:"parentId"`
	Children    []*RoleRow `col:"CHILDREN,tree,ID+PARENT_ID" json:"children"`
	Policies    []*Policy  `col:"POLICIES,json" json:"policies"`
	Version     int64      `col:"VERSION,aol" json:"version"`
}

func (t *RoleRow) TableName() (schema string, table string) {
	schema = _roleSchema
	table = _roleTable
	return
}

type UserRoleRow struct {
	dal.Audits
	Roles   []string `col:"ROLES,json" json:"roles"`
	Version int64    `col:"VERSION,aol" json:"version"`
}

func (t *UserRoleRow) TableName() (schema string, table string) {
	schema = _userSchema
	table = _userTable
	return
}
