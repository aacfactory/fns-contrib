package conditions_test

import (
	"fmt"
	"reflect"
)

func NewDict(ss ...string) Dict {
	dict := make(Dict)
	for i := 0; i < len(ss); i += 2 {
		dict[ss[i]] = []byte(ss[i+1])
	}
	return dict
}

type Dict map[string][]byte

func (dict Dict) Get(key ...any) (value []byte, has bool) {
	keyLen := len(key)
	if keyLen == 0 || keyLen > 2 {
		return
	}
	rv := reflect.Indirect(reflect.ValueOf(key[0]))
	rt := rv.Type()
	st := ""
	if rt.Kind() == reflect.Struct {
		st = fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	} else {
		st = fmt.Sprintf("%s", key[0])
	}

	if keyLen == 1 {
		value, has = dict[st]
		return
	}
	value, has = dict[fmt.Sprintf("%s:%s", st, key[1])]
	return
}

type QueryPlaceholder struct {
	count int
}

func (q *QueryPlaceholder) Next() (v []byte) {
	q.count++
	return []byte(fmt.Sprintf("$%d", q.count))
}
