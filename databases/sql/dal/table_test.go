package dal

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"testing"
	"time"
)

type Audits struct {
	Id       string    `col:"ID,pk" json:"ID"`
	CreateBY string    `col:"CREATE_BY,acb" json:"CREATE_BY"`
	CreateAT time.Time `col:"CREATE_AT,act" json:"CREATE_AT"`
	ModifyBY string    `col:"MODIFY_BY,amb" json:"MODIFY_BY"`
	ModifyAT time.Time `col:"MODIFY_AT,amt" json:"MODIFY_AT"`
	DeleteBY string    `col:"DELETE_BY,adb" json:"DELETE_BY"`
	DeleteAT time.Time `col:"DELETE_AT,adt" json:"DELETE_AT"`
	Version  int64     `col:"VERSION,aol" json:"VERSION"`
}

type Avatar struct {
	Icon   string `json:"icon"`
	Width  int64  `json:"width"`
	Height int64  `json:"height"`
}

type User struct {
	Audits
	Name     string   `col:"NAME" json:"NAME"`
	Age      int      `col:"AGE" json:"AGE"`
	Birthday sql.Date `col:"BIRTHDAY" json:"BIRTHDAY"`
	Avatar   *Avatar  `col:"AVATAR,json" json:"AVATAR"`
	Group    *Group   `col:"GROUP,ref,GROUP_ID+ID" json:"GROUP"`
}

func (user *User) TableName() (schema string, name string) {
	schema, name = "FNS", "USER"
	return
}

type Group struct {
	Audits
	Name     string   `col:"NAME" json:"NAME"`
	ParentId string   `col:"PARENT_ID" json:"PARENT_ID"`
	Users    []*User  `col:"USERS,links,ID+GROUP_ID,NAME ASC" json:"USERS,links"`
	Children []*Group `col:"-,tree,ID+PARENT_ID" json:"-"`
	Leader   *User    `col:"LEADER,ref,LEADER_ID+ID" json:"LEADER"`
	Members  int64    `col:"MEMBERS,vc,{query}" json:"MEMBERS"`
}

func (group *Group) TableName() (schema string, name string) {
	schema, name = "FNS", "GROUP"
	return
}

func TestModel(t *testing.T) {
	user := &User{}
	structure, getStructureErr := getModelStructure(user)
	if getStructureErr != nil {
		t.Errorf("%+v", getStructureErr)
		return
	}
	fmt.Println(structure.schema, structure.name)
	for _, field := range structure.fields {
		fmt.Println(field.name, field.kind, field.reference)
	}
	fmt.Println("---")
	group := &Group{}
	structure, getStructureErr = getModelStructure(group)
	if getStructureErr != nil {
		t.Errorf("%+v", getStructureErr)
		return
	}
	fmt.Println(structure.schema, structure.name)
	for _, field := range structure.fields {
		fmt.Println(field.name, field.kind, field)
		if field.reference != nil {
			fmt.Println("\t", field.reference)
		}
		if field.link != nil {
			fmt.Println("\t", field.link, field.link.orders.values[0], field.link.rng)
		}
		fmt.Println("\t", field.virtual)
		fmt.Println("\t", field.tree)
	}
}
