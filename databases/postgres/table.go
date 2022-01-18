package postgres

import (
	"fmt"
	"go/ast"
	"golang.org/x/sync/singleflight"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	tagName = "col"
)

type Table interface {
	TableName() (schema string, table string)
}

var (
	tables     = new(sync.Map)
	tableType  = reflect.TypeOf((*Table)(nil)).Elem()
	tableGroup = new(singleflight.Group)
)

func isImplementTable(typ reflect.Type) bool {
	return typ.Implements(tableType)
}

func getTable(typ reflect.Type) (v *table, has bool) {
	key := fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
	stored, hasStored := tables.Load(key)
	if hasStored {
		has = true
		v = stored.(*table)
		return
	}
	return
}

func setTable(typ reflect.Type, v *table) {
	key := fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
	tables.Store(key, v)
}

func createOrLoadTable(x interface{}) (v *table) {
	rt := reflect.TypeOf(x)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	if !isImplementTable(rt) {
		panic(fmt.Sprintf("fns postgres: analyse %s failed, type of it is not Table", key))
		return
	}
	stored, hasStored := getTable(rt)
	if hasStored {
		v = stored
		return
	}
	v = createTable(x)
	return
}

func createTable(x interface{}) (v *table) {
	rt := reflect.TypeOf(x)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	r, _, _ := tableGroup.Do(key, func() (r interface{}, err error) {
		target, typeOk := x.(Table)
		if !typeOk {
			panic(fmt.Sprintf("fns postgres: analyse %s failed, type of it is not Table", key))
			return
		}
		schema, tableName := target.TableName()
		schema = strings.TrimSpace(schema)
		tableName = strings.TrimSpace(tableName)
		if tableName == "" {
			panic(fmt.Sprintf("fns postgres: analyse %s failed, table name is empty", key))
			return
		}
		if schema == "" {
			schema = "public"
		}

		fieldNum := rt.NumField()
		if fieldNum == 0 {
			panic(fmt.Sprintf("fns postgres: analyse %s failed, no field", key))
			return
		}

		v = &table{
			Schema:  schema,
			Name:    tableName,
			Columns: make([]*column, 0, 1),
		}

		setTable(rt, v)

		for i := 0; i < fieldNum; i++ {
			err = v.addColumn(rt.Field(i))
			if err != nil {
				panic(fmt.Sprintf("fns postgres: analyse %s failed, %v", key, err))
				return
			}
		}

		if len(v.Columns) == 0 {
			panic(fmt.Sprintf("fns postgres: analyse %s failed, no columns", key))
			return
		}
		r = v
		return
	})

	v = r.(*table)

	return
}

type table struct {
	Schema  string
	Name    string
	Columns []*column
}

func (t *table) addColumn(field reflect.StructField) (err error) {
	fieldName := field.Name
	if !ast.IsExported(fieldName) {
		return
	}
	tag, hasTag := field.Tag.Lookup(tagName)
	if !hasTag {
		return
	}
	tag = strings.TrimSpace(tag)
	if tag == "" {
		err = fmt.Errorf("%s has col tag but no content", fieldName)
		return
	}
	if tag == "-" {
		return
	}

	tagItems := strings.Split(tag, ",")
	columnName := strings.TrimSpace(tagItems[0])
	if len(tagItems) == 1 {
		// normal
		t.Columns = append(t.Columns, newColumn(t, normal, columnName, fieldName))
		return
	}
	kind := strings.TrimSpace(tagItems[1])
	switch kind {
	case pkCol:
		t.Columns = append(t.Columns, newColumn(t, pkCol, columnName, fieldName))
	case incrPkCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(0)) {
			err = fmt.Errorf("%s is incr pk, type must be int64", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, incrPkCol, columnName, fieldName))
	case normal:
		t.Columns = append(t.Columns, newColumn(t, normal, columnName, fieldName))
	case jsonCol:
		t.Columns = append(t.Columns, newColumn(t, jsonCol, columnName, fieldName))
	case auditCreateByCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit create by, type must be string", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditCreateByCol, columnName, fieldName))
	case auditCreateAtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s is audit create at, type must be time.Time", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditCreateAtCol, columnName, fieldName))
	case auditModifyBtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit modify by, type must be string", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditModifyBtCol, columnName, fieldName))
	case auditModifyAtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s is audit modify at, type must be time.Time", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditModifyAtCol, columnName, fieldName))
	case auditDeleteByCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit delete by, type must be string", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditDeleteByCol, columnName, fieldName))
	case auditDeleteAtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s is audit delete at, type must be time.Time", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditDeleteAtCol, columnName, fieldName))
	case auditVersionCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit version, type must be int64", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditVersionCol, columnName, fieldName))
	case virtualCol:
		if len(tagItems) < 3 {
			err = fmt.Errorf("%s is vc, source sql must be setted", fieldName)
			return
		}
		sourceSQL := strings.TrimSpace(tagItems[2])
		col := newColumn(t, virtualCol, columnName, fieldName)
		col.VirtualQuery = sourceSQL
		t.Columns = append(t.Columns, col)
	case refCol:
		fieldType := field.Type
		if fieldType.Kind() != reflect.Ptr {
			err = fmt.Errorf("%s is ref, field type must point struct", fieldName)
			return
		}
		refType := fieldType.Elem()
		if refType.Kind() != reflect.Struct {
			err = fmt.Errorf("%s is ref, field type must point struct", fieldName)
			return
		}
		refTable := createOrLoadTable(reflect.New(refType))
		col := newColumn(t, refCol, columnName, fieldName)
		col.Ref = refTable
		t.Columns = append(t.Columns, col)
	case linkCol:
		fieldType := field.Type
		if fieldType.Kind() != reflect.Ptr {
			err = fmt.Errorf("%s is link, field type must point struct", fieldName)
			return
		}
		linkType := fieldType.Elem()
		if linkType.Kind() != reflect.Struct {
			err = fmt.Errorf("%s is link, field type must point struct", fieldName)
			return
		}
		linkTable := createOrLoadTable(reflect.New(linkType))
		col := newColumn(t, linkCol, columnName, fieldName)
		col.Link = linkTable
		t.Columns = append(t.Columns, col)
	case linksCol:
		fieldType := field.Type
		if !(fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Array) {
			err = fmt.Errorf("%s is links, field type must slice point struct", fieldName)
			return
		}
		itemType := fieldType.Elem()
		if itemType.Kind() != reflect.Ptr {
			err = fmt.Errorf("%s is links, field type must slice point struct", fieldName)
			return
		}
		linkType := itemType.Elem()
		if linkType.Kind() != reflect.Struct {
			err = fmt.Errorf("%s is links, field type must slice point struct", fieldName)
			return
		}
		linkTable := createOrLoadTable(reflect.New(linkType))
		col := newColumn(t, linksCol, columnName, fieldName)
		col.Link = linkTable
		if len(tagItems) > 2 {
			settings := tagItems[2:]
			for _, setting := range settings {
				setting = strings.TrimSpace(setting)
				if ridx := strings.Index(setting, ":"); ridx > 0 {
					// range
					offset := strings.TrimSpace(setting[0:ridx])
					limit := strings.TrimSpace(setting[ridx+1:])
					col.LinkRange = []string{offset, limit}
				} else {
					// orders
					col.LinkOrders = make([]string, 0, 1)
					orders := strings.Split(setting, " ")
					if len(orders) == 1 {
						col.LinkOrders = append(col.LinkOrders, setting, "ASC")
					} else {
						orderField := orders[0]
						orderKind := ""
						for i := 1; i < len(orders); i++ {
							orderKind0 := orders[i]
							if orderKind0 == "" {
								continue
							}
							orderKind = orderKind0
							break
						}
						if orderKind == "" {
							col.LinkOrders = append(col.LinkOrders, orderField, "ASC")
						} else {
							col.LinkOrders = append(col.LinkOrders, orderField, strings.ToUpper(orderKind))
						}
					}
				}
			}
		}
		t.Columns = append(t.Columns, col)
	default:
		err = fmt.Errorf("%s has col tag but kind(%s) is not supported", fieldName, kind)
		return
	}
	return
}

func (t *table) fullName() (v string) {
	if t.Schema == "" || t.Schema == "public" {
		v = fmt.Sprintf("public.\"%s\"", t.Name)
		return
	}
	v = fmt.Sprintf("\"%s\".\"%s\"", t.Schema, t.Name)
	return
}

func (t *table) generateInsertSQL() (query string, columns []*column) {

	return
}

func (t *table) generateInsertWhenExistOrNotSQL(exist bool, sourceSQL string) (query string, columns []*column) {

	return
}

func (t *table) generateUpdateSQL() (query string, columns []*column) {

	return
}

func (t *table) generateDeleteSQL() (query string, columns []*column) {

	return
}

func (t *table) generateInsertOrUpdateSQL() (query string, columns []*column) {

	return
}

func (t *table) generateExistSQL(conditions *Conditions) (query string) {

	return
}

func (t *table) generateCountSQL(conditions *Conditions) (query string) {

	return
}

func (t *table) generateSelects() (query string) {
	for _, c := range t.Columns {
		if c.VirtualQuery == "" {
			query = query + ", " + "\"" + c.Name + "\""
		} else {
			query = query + ", " + "(" + c.VirtualQuery + ")" + " AS " + "\"" + c.Name + "\""
		}
	}
	query = query[1:]
	return
}

func (t *table) generateQuerySQL(conditions *Conditions, rng *Range, orders []*Order) (query string) {

	return
}
