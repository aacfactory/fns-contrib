package postgres_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/postgres"
	"reflect"
	"testing"
	"time"
)

type Date time.Time

type Foo struct {
}

func (f Foo) TableName() (schema string, table string) {
	return
}

func TestConvert(t *testing.T) {
	x := reflect.TypeOf(time.Time{})
	y := reflect.TypeOf(Date{})
	fmt.Println(y.AssignableTo(x), y.ConvertibleTo(x))
	fmt.Println(reflect.TypeOf(Foo{}).Implements(reflect.TypeOf((*postgres.Table)(nil)).Elem()))
}
