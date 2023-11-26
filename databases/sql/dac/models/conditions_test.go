package models_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/models"
	"reflect"
	"testing"
	"time"
)

func NewDict(ss ...string) models.Dict {
	dict := make(models.Dict)
	for i := 0; i < len(ss); i += 2 {
		dict[ss[i]] = []byte(ss[i+1])
	}
	return dict
}

type QueryPlaceholder struct {
	count int
}

func (q *QueryPlaceholder) Next() (v []byte) {
	q.count++
	return []byte(fmt.Sprintf("$%d", q.count))
}

type User struct {
	Id       string    `column:"id"`
	Name     string    `column:"name"`
	Age      int       `column:"age"`
	Birthday time.Time `column:"birthday"`
	PostId   string    `column:"post_id"`
}

func TestCondition_Render(t *testing.T) {
	ut := reflect.TypeOf(User{})
	pfx := fmt.Sprintf("%s.%s", ut.PkgPath(), ut.Name())
	dict := NewDict(
		pfx, "user",
		pfx+":Id", "id",
		pfx+":Name", "name",
		pfx+":Age", "age",
		pfx+":Birthday", "birthday",
		pfx+":PostId", "post_id",
	)
	ctx := models.Todo(context.TODO(), User{}, dict, &QueryPlaceholder{})

	cond := conditions.New(conditions.Eq("Id", 1))
	cond = cond.And(conditions.Eq("Name", "name"))
	cond = cond.And(conditions.New(conditions.Eq("Age", 2)).Or(conditions.Eq("Birthday", "2")))
	cond = cond.And(conditions.Eq("Name", sql.Named("foo", "bar")))

	buf := bytes.NewBuffer([]byte{})

	args, err := models.Condition{Condition: cond}.Render(ctx, buf)
	if err != nil {
		fmt.Println(fmt.Sprintf("%+v", err))
		return
	}

	fmt.Println(buf.String())
	fmt.Println(fmt.Sprintf("%+v", args))
}

func TestCondition_RenderLit(t *testing.T) {
	ut := reflect.TypeOf(User{})
	pfx := fmt.Sprintf("%s.%s", ut.PkgPath(), ut.Name())
	dict := NewDict(
		pfx, "user",
		pfx+":Id", "id",
		pfx+":Name", "name",
		pfx+":Age", "age",
		pfx+":Birthday", "birthday",
		pfx+":PostId", "post_id",
	)
	ctx := models.Todo(context.TODO(), User{}, dict, &QueryPlaceholder{})

	cond := conditions.New(conditions.Eq("Id", 1))
	cond = cond.And(conditions.Eq("Name", conditions.String("name")))
	cond = cond.And(conditions.Eq("Age", conditions.Int(13)))
	cond = cond.And(conditions.Eq("Birthday", conditions.Time(time.Now())))
	cond = cond.And(conditions.Eq("Birthday", conditions.LitQuery(`select now()`)))

	buf := bytes.NewBuffer([]byte{})

	args, err := models.Condition{Condition: cond}.Render(ctx, buf)
	if err != nil {
		fmt.Println(fmt.Sprintf("%+v", err))
		return
	}

	fmt.Println(buf.String())
	fmt.Println(fmt.Sprintf("%+v", args))
}

type Post struct {
	Id int
}

func TestCondition_RenderQuery(t *testing.T) {
	ut := reflect.TypeOf(User{})
	pfx := fmt.Sprintf("%s.%s", ut.PkgPath(), ut.Name())
	pt := reflect.TypeOf(Post{})
	ptx := fmt.Sprintf("%s.%s", pt.PkgPath(), pt.Name())
	dict := NewDict(
		pfx, "user",
		pfx+":Id", "id",
		pfx+":Name", "name",
		pfx+":Age", "age",
		pfx+":Birthday", "birthday",
		pfx+":PostId", "post_id",
		ptx, "post",
		ptx+":Id", "pid",
	)

	ctx := models.Todo(context.TODO(), User{}, dict, &QueryPlaceholder{})

	cond := conditions.New(conditions.Eq("Id", 1))
	cond = cond.And(conditions.Eq("Name", "name"))
	cond = cond.And(conditions.In("PostId", conditions.Query(User{}, "Id", conditions.New(conditions.Eq("Id", 2)))))
	cond = cond.And(conditions.In("PostId", "1", "2", "3"))

	buf := bytes.NewBuffer([]byte{})

	args, err := models.Condition{Condition: cond}.Render(ctx, buf)
	if err != nil {
		fmt.Println(fmt.Sprintf("%+v", err))
		return
	}

	fmt.Println(buf.String())
	fmt.Println(fmt.Sprintf("%+v", args))

}
