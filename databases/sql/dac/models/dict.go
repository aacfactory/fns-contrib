package models

import (
	"fmt"
	"reflect"
)

type Dict map[string][]byte

func (dict Dict) Set(table Table) (ok bool) {
	if table == nil {
		return
	}
	info := table.TableInfo()
	tableName := info.Name
	if tableName == "" {
		return
	}
	if info.Schema != "" {
		tableName = info.Schema + "." + tableName
	}

	rv := reflect.Indirect(reflect.ValueOf(table))
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return
	}
	st := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())

	fields := getColumnFields(rt)
	fieldsLen := len(fields)
	if fieldsLen == 0 {
		return
	}
	dict[st] = []byte(tableName)
	for i := 0; i < fieldsLen; i += 2 {
		field := fields[i]
		column := fields[i+1]
		dict[fmt.Sprintf("%s:%s", st, field)] = []byte(column)
	}
	ok = true
	return
}

func (dict Dict) Get(key ...any) (value []byte, has bool) {
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
		value, has = dict[st]
		return
	}
	value, has = dict[fmt.Sprintf("%s:%s", st, key[1])]
	return
}

func getColumnFields(rt reflect.Type) (fields []string) {
	n := rt.NumField()
	for i := 0; i < n; i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		if field.Anonymous {
			fields = append(fields, getColumnFields(field.Type)...)
			continue
		}
		column, _, ok := getColumn(field)
		if ok {
			fields = append(fields, field.Name, column)
		}
	}
	return
}
