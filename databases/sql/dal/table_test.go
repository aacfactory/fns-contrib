package dal_test

import (
	"context"
	"fmt"
	"github.com/aacfactory/copier"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

type Foo struct {
	Bar
	Id   string
	Name string
}

func (foo *Foo) TableName() (schema string, name string) {
	return
}

type Bar struct {
	Age int
}

func TestRV(t *testing.T) {
	foo := &Foo{}
	rt := reflect.ValueOf(foo)
	rv(rt)
	fmt.Println(rt.Elem().Interface())
	rt.Elem().FieldByName("Id").SetString("foo")
	fmt.Println(rt.Elem().Interface())
}

func rv(value reflect.Value) {
	value.Elem().FieldByName("Id").SetString("foo")
}

func TestReflect(t *testing.T) {
	foo := Foo{}
	rt := reflect.TypeOf(foo)
	fmt.Println(rt.Kind(), rt.Kind())
	fieldNum := rt.NumField()
	for i := 0; i < fieldNum; i++ {
		field := rt.Field(i)
		fmt.Println("field:", field.Name, field.Type.Kind(), field.Anonymous)
	}
	//var bar *Bar
	bt := reflect.TypeOf(nil)
	fmt.Println(bt, bt == nil)
	fmt.Println("---")
}

func TestCopy(t *testing.T) {
	a := &Bar{}
	rv := reflect.ValueOf(a)
	rt := rv.Type().Elem()
	bt := reflect.NewAt(rt, rv.UnsafePointer())
	b0 := bt.Elem().Interface().(Bar)
	b := &b0
	fmt.Println(a, reflect.ValueOf(a).UnsafePointer(), reflect.ValueOf(a).Pointer())
	fmt.Println(b, reflect.ValueOf(b).UnsafePointer(), reflect.ValueOf(b).Pointer())
}

func TestAsc(t *testing.T) {
	query := "SELECT XX, FDASFASF, ASWER FROM AAA"
	fromIdx := strings.LastIndex(query, " FROM ")
	query = "SELECT x" + query[fromIdx:]
	fmt.Println(query)
}

func TestBetween(t *testing.T) {
	dsn := "oracle://user:pass@server/service_name"
	u, parserErr := url.Parse(dsn)
	if parserErr != nil {
		t.Error(parserErr)
		return
	}
	fmt.Println(u.Scheme, u.Host)
}

func TestRange_MapToPager(t *testing.T) {
	r := dal.NewRange(0, 10)
	fmt.Println(r.MapToPager(), 1)
	r = dal.NewRange(1, 10)
	fmt.Println(r.MapToPager(), 1)
	r = dal.NewRange(10, 10)
	fmt.Println(r.MapToPager(), 2)
	r = dal.NewRange(11, 10)
	fmt.Println(r.MapToPager(), 2)
	r = dal.NewRange(20, 10)
	fmt.Println(r.MapToPager(), 3)
}

func TestPager_MapToRange(t *testing.T) {
	p := dal.NewPager(1, 10)
	fmt.Println(p.MapToRange(), 0)
	p = dal.NewPager(2, 10)
	fmt.Println(p.MapToRange(), 10)
	p = dal.NewPager(3, 10)
	fmt.Println(p.MapToRange(), 20)
}

type FooModel struct {
	Id string `col:"ID,pk" json:"ID,pk"`
}

func (model *FooModel) TableName() (schema string, name string) {
	schema = "sss"
	name = "foo"
	return
}

func TestCount(t *testing.T) {
	_, _ = dal.Count[*FooModel](context.TODO(), nil)
}

func TestCp(t *testing.T) {
	ss := make([]*Foo, 0, 1)
	ss = append(ss, &Foo{
		Bar:  Bar{},
		Id:   "1",
		Name: "",
	})
	fmt.Println(reflect.ValueOf(ss[0]).Pointer(), ss[0])
	fmt.Println(copier.Copy(ss[0], &Foo{
		Bar:  Bar{},
		Id:   "1",
		Name: "2",
	}))
	fmt.Println(reflect.ValueOf(ss[0]).Pointer(), ss[0])
}
