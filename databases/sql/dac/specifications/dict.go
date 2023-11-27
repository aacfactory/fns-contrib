package specifications

import (
	"fmt"
	"reflect"
	"sync"
)

func NewDict() *Dict {
	return &Dict{
		values: sync.Map{},
	}
}

type Dict struct {
	values sync.Map
}

func (dict *Dict) Get(key ...any) (value [][]byte, has bool) {
	keyLen := len(key)
	if keyLen == 0 || keyLen > 2 {
		return
	}
	rv := reflect.Indirect(reflect.ValueOf(key[0]))
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return
	}
	st := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	if keyLen == 1 {
		v, exist := dict.values.Load(st)
		if exist {
			value, has = v.([][]byte)
		}
		return
	}
	v, exist := dict.values.Load(fmt.Sprintf("%s:%s", st, key[1]))
	if exist {
		value, has = v.([][]byte)
	}
	return
}

// Set
// table: key is {path}.{name}, value is table name
// column: key is {path}.{name}:{field}, value is column name
func (dict *Dict) Set(key string, value ...[]byte) {
	dict.values.Store(key, value)
	return
}

func DictSet(key string, value ...[]byte) {
	dict.Set(key, value...)
}
