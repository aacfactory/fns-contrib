package sql_test

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"
)

type Column struct {
	Name string
}

type ColumnScanner struct {
	Column
}

func TestRows(t *testing.T) {
	a := make([]interface{}, 0, 1)
	a = append(a, &ColumnScanner{
		Column{Name: "a"},
	})

	rv := reflect.ValueOf(&a)
	fmt.Println(rv.Pointer())

	b := reflect.NewAt(reflect.SliceOf(reflect.TypeOf(&Column{})), unsafe.Pointer(reflect.ValueOf(&a).Pointer()))

	fmt.Println(b, b.Pointer())
	fmt.Println(b.Interface())
	fmt.Println(b.Elem().Interface().([]*Column))

}
