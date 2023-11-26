package conditions_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"reflect"
	"testing"
	"time"
)

func TestCondition_Render(t *testing.T) {
	dict := map[string]string{
		"Id":       "id",
		"Name":     "name",
		"Age":      "age",
		"Birthday": "birthday",
	}
	ctx := RenderTODO(dict)

	cond := conditions.New(conditions.Eq("Id", 1))
	cond = cond.And(conditions.Eq("Name", "name"))
	cond = cond.And(conditions.New(conditions.Eq("Age", 2)).Or(conditions.Eq("Birthday", "2")))

	buf := bytes.NewBuffer([]byte{})

	args, err := cond.Render(ctx, buf)
	if err != nil {
		fmt.Println(fmt.Sprintf("%+v", err))
		return
	}

	fmt.Println(buf.String())
	fmt.Println(fmt.Sprintf("%+v", args))
}

func TestCondition_RenderLit(t *testing.T) {
	dict := map[string]string{
		"Id":       "id",
		"Name":     "name",
		"Age":      "age",
		"Birthday": "birthday",
	}
	ctx := RenderTODO(dict)
	cond := conditions.New(conditions.Eq("Id", 1))
	cond = cond.And(conditions.Eq("Name", conditions.String("name")))
	cond = cond.And(conditions.Eq("Age", conditions.Int(13)))
	cond = cond.And(conditions.Eq("Birthday", conditions.Time(time.Now())))
	cond = cond.And(conditions.Eq("Birthday", conditions.LitQuery(`select now()`)))

	buf := bytes.NewBuffer([]byte{})

	args, err := cond.Render(ctx, buf)
	if err != nil {
		fmt.Println(fmt.Sprintf("%+v", err))
		return
	}

	fmt.Println(buf.String())
	fmt.Println(fmt.Sprintf("%+v", args))
}

// User
// user table
type User struct {
	Id int
}

func TestCondition_RenderQuery(t *testing.T) {
	ut := reflect.TypeOf(User{})
	dict := map[string]string{
		"Id":       "id",
		"Name":     "name",
		"Age":      "age",
		"Birthday": "birthday",
		"UserId":   "user_id",
		fmt.Sprintf("%s.%s", ut.PkgPath(), ut.Name()): `"users"."user"`,
	}
	ctx := RenderTODO(dict)

	cond := conditions.New(conditions.Eq("Id", 1))
	cond = cond.And(conditions.Eq("Name", "name"))
	cond = cond.And(conditions.In("UserId", conditions.Query(User{}, "Id", conditions.New(conditions.Eq("Id", 2)))))
	cond = cond.And(conditions.In("UserId", "1", "2", "3"))

	buf := bytes.NewBuffer([]byte{})

	args, err := cond.Render(ctx, buf)
	if err != nil {
		fmt.Println(fmt.Sprintf("%+v", err))
		return
	}

	fmt.Println(buf.String())
	fmt.Println(fmt.Sprintf("%+v", args))

}
