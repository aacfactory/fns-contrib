package postgres

import (
	"container/list"
	"github.com/aacfactory/fns/service/builtin/permissions"
)

type ModelRow struct {
	Name      string         `col:"NAME,pk" json:"NAME"`
	Parent    string         `col:"PARENT" json:"PARENT"`
	Resources map[string]int `col:"RESOURCES,json" json:"RESOURCES"`
	Version   int64          `col:"VERSION,aol"`
	schema    string
	table     string
}

func (row ModelRow) TableName() (schema string, table string) {
	schema = row.schema
	table = row.table
	return
}

func modelsMapToRoles(models *list.List, parent string) (roles []*permissions.Role) {
	if models.Len() == 0 {
		return
	}
	roles = make([]*permissions.Role, 0, 1)
	element := models.Front()
	for {
		if element.Value == nil {
			break
		}
		model := element.Value.(*ModelRow)
		if model.Parent == parent {
			role := &permissions.Role{
				Name:      model.Name,
				Parent:    model.Parent,
				Children:  nil,
				Resources: model.Resources,
			}
			models.Remove(element)
			children := modelsMapToRoles(models, role.Name)
			if children != nil && len(children) > 0 {
				role.Children = children
			}
			roles = append(roles, role)
		}
		element = element.Next()
	}
	return
}

type PolicyRow struct {
	UserId  string   `col:"USER_ID,pk" json:"USER_ID"`
	Roles   []string `col:"ROLES,json" json:"ROLES"`
	Version int64    `col:"VERSION,aol"`
	schema  string
	table   string
}

func (row PolicyRow) TableName() (schema string, table string) {
	schema = row.schema
	table = row.table
	return
}
