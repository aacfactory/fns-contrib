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
	Id       string    `col:"ID,pk" json:"id"`
	CreateBY string    `col:"CREATE_BY,acb" json:"createBY"`
	CreateAT time.Time `col:"CREATE_AT,act" json:"createAT"`
	ModifyBY string    `col:"MODIFY_BY,amb" json:"modifyBY"`
	ModifyAT time.Time `col:"MODIFY_AT,amt" json:"modifyAT"`
	DeleteBY string    `col:"DELETE_BY,adb" json:"deleteBY"`
	DeleteAT time.Time `col:"DELETE_AT,adt" json:"deleteAT"`
	Version  int64     `col:"VERSION,aol" json:"version"`
}

type Avatar struct {
	Icon   string `json:"icon"`
	Width  int64  `json:"width"`
	Height int64  `json:"height"`
}

type User struct {
	Audits
	Name     string   `col:"NAME,conflict" json:"name"`
	Age      int      `col:"AGE" json:"age"`
	Birthday sql.Date `col:"BIRTHDAY" json:"birthday"`
	Avatar   *Avatar  `col:"AVATAR,json" json:"avatar"`
	Group    *Group   `col:"GROUP,ref,GROUP_ID+ID" json:"group"`
}

func (user *User) TableName() (schema string, name string) {
	schema, name = "FNS", "USER"
	return
}

type Group struct {
	Audits
	Name     string   `col:"NAME" json:"name"`
	ParentId string   `col:"PARENT_ID" json:"parentId"`
	Users    []*User  `col:"USERS,links,ID+GROUP_ID,NAME ASC" json:"users"`
	Children []*Group `col:"-,tree,ID+PARENT_ID" json:"-"`
	Leader   *User    `col:"LEADER,ref,LEADER_ID+ID" json:"leader"`
	Members  int64    `col:"MEMBERS,vc,{query}" json:"members"`
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
	fmt.Println(generator.Select(context.TODO(), dal.NewConditions(dal.Eq("NAME", "NAME")), dal.NewOrders().Desc("AGE"), dal.NewRange(0, 12)))
	fmt.Println(generator.Count(context.TODO(), dal.NewConditions(dal.Between("AGE", 10, 12))))
	fmt.Println(generator.Exist(context.TODO(), dal.NewConditions(dal.IN("NAME", []string{"foo", "bar"}))))
	fmt.Println(generator.Exist(context.TODO(), dal.NewConditions(
		dal.Eq("GROUP_ID", "G2")).And(dal.IN("NAME", dal.NewSubQueryArgument(&User{}, "NAME", dal.NewConditions(dal.Eq("GROUP_ID", "g1")))))))
	fmt.Println(generator.Insert(context.TODO(), &User{}))
	fmt.Println(generator.Update(context.TODO(), &User{}))
	fmt.Println(generator.Delete(context.TODO(), &User{}))
	fmt.Println(generator.InsertOrUpdate(context.TODO(), &User{}))
	fmt.Println(generator.InsertWhenExist(context.TODO(), &User{}, "{source}"))
	fmt.Println(generator.InsertWhenNotExist(context.TODO(), &User{}, "{source}"))

}

type UserRow struct {
	Audits
	Nickname string    `col:"NICKNAME" json:"nickname"`
	Mobile   string    `col:"MOBILE" json:"mobile"`
	Gender   string    `col:"GENDER" json:"gender"`
	Birthday time.Time `col:"BIRTHDAY" json:"birthday"`
}

func (r *UserRow) TableName() (string, string) {
	return "FNS", "USER"
}

type PostRow struct {
	Id       string    `col:"ID,pk" json:"id"`
	User     *UserRow  `col:"USER,ref,USER_ID+ID" json:"user"`
	CreateAT time.Time `col:"CREATE_AT,act" json:"createAT"`
	Version  int64     `col:"VERSION,aol" json:"version"`
	Title    string    `col:"TITLE" json:"title"`
	Content  string    `col:"CONTENT" json:"content"`
	Likes    int64     `col:"LIKES,vc,SELECT COUNT(1) FROM \"FNS\".\"POST_LIKE\" WHERE \"POST_ID\" = \"FNS\".\"POST\".\"ID\"" json:"likes"`
}

func (r *PostRow) TableName() (string, string) {
	return "FNS", "POST"
}

func TestReference(t *testing.T) {
	builder := &postgres.QueryGeneratorBuilder{}
	st, stErr := dal.StructureOfModel(&PostRow{})
	if stErr != nil {
		t.Errorf("%+v", stErr)
		return
	}
	generator, buildErr := builder.Build(st)
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	method, query, args, err := generator.Select(context.TODO(), dal.NewConditions(dal.Eq("ID", "")), dal.NewOrders().Asc("ID"), dal.NewRange(0, 10))
	if err != nil {
		t.Errorf("%+v", err)
		return
	}
	fmt.Println(method)
	fmt.Println(query)
	fmt.Println(args, len(args))
}

func TestUpdateWithValues(t *testing.T) {
	builder := &postgres.QueryGeneratorBuilder{}
	st, stErr := dal.StructureOfModel(&PostRow{})
	if stErr != nil {
		t.Errorf("%+v", stErr)
		return
	}
	generator, buildErr := builder.Build(st)
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	method, query, args, err := generator.UpdateWithValues(
		context.TODO(),
		dal.NewValues().Append("TITLE", "newtile").Append("CONTENT", dal.NewUnpreparedValue("'fff'")),
		dal.NewConditions(dal.Eq("ID", "1")),
	)
	if err != nil {
		t.Errorf("%+v", err)
		return
	}
	fmt.Println(method)
	fmt.Println(query)
	fmt.Println(args, len(args))
}

func TestDeleteWithConditions(t *testing.T) {
	builder := &postgres.QueryGeneratorBuilder{}
	st, stErr := dal.StructureOfModel(&PostRow{})
	if stErr != nil {
		t.Errorf("%+v", stErr)
		return
	}
	generator, buildErr := builder.Build(st)
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	method, query, args, err := generator.DeleteWithConditions(
		context.TODO(),
		dal.NewConditions(dal.Eq("ID", "1")),
	)
	if err != nil {
		t.Errorf("%+v", err)
		return
	}
	fmt.Println(method)
	fmt.Println(query)
	fmt.Println(args, len(args))
}
