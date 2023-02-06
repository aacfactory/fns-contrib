package postgres_test

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/fns-contrib/databases/sql/dal/postgres"
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
	Name     string   `col:"NAME,conflict" json:"NAME"`
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

func TestQueryGeneratorBuilder(t *testing.T) {
	builder := &postgres.QueryGeneratorBuilder{}
	st, stErr := dal.StructureOfModel(&User{})
	if stErr != nil {
		t.Errorf("%+v", stErr)
		return
	}
	generator, buildErr := builder.Build(st)
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	fmt.Println(generator.Query(context.TODO(), dal.NewConditions(dal.Eq("NAME", "NAME")), dal.NewOrders().Desc("AGE"), dal.NewRange(0, 12)))
	fmt.Println(generator.Count(context.TODO(), dal.NewConditions(dal.Between("AGE", 10, 12))))
	fmt.Println(generator.Exist(context.TODO(), dal.NewConditions(dal.IN("NAME", []string{"foo", "bar"}))))
	fmt.Println(generator.Exist(context.TODO(), dal.NewConditions(dal.IN("NAME", dal.NewSubQueryArgument(&User{}, "NAME", dal.NewConditions(dal.Eq("GROUP_ID", "g1")))))))
	fmt.Println(generator.Insert(context.TODO(), &User{}))
	fmt.Println(generator.Update(context.TODO(), &User{}))
	fmt.Println(generator.Delete(context.TODO(), &User{}))
	fmt.Println(generator.InsertOrUpdate(context.TODO(), &User{}))
	fmt.Println(generator.InsertWhenExist(context.TODO(), &User{}, "{source}"))
	fmt.Println(generator.InsertWhenNotExist(context.TODO(), &User{}, "{source}"))
}
