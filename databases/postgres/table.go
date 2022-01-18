package postgres

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"go/ast"
	"golang.org/x/sync/singleflight"
	"reflect"
	"strconv"
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

		insertQuery, insertColumns := v.generateInsertSQL()
		if insertQuery != "" {
			v.insertQuery = &tableGenericQuery{
				query:   insertQuery,
				columns: insertColumns,
			}
		}
		updateQuery, updateColumns := v.generateUpdateSQL()
		if updateQuery != "" {
			v.updateQuery = &tableGenericQuery{
				query:   updateQuery,
				columns: updateColumns,
			}
		}
		deleteQuery, deleteColumns := v.generateDeleteSQL()
		if deleteQuery != "" {
			v.deleteQuery = &tableGenericQuery{
				query:   deleteQuery,
				columns: deleteColumns,
			}
		}
		softDeleteQuery, softDeleteColumns := v.generateSoftDeleteSQL()
		if softDeleteQuery != "" {
			v.softDeleteQuery = &tableGenericQuery{
				query:   softDeleteQuery,
				columns: softDeleteColumns,
			}
		}
		v.querySelects = v.generateQuerySelects()
		r = v
		return
	})

	v = r.(*table)

	return
}

type tableGenericQuery struct {
	query   string
	columns []*column
}

type table struct {
	Schema              string
	Name                string
	Columns             []*column
	insertQuery         *tableGenericQuery
	updateQuery         *tableGenericQuery
	deleteQuery         *tableGenericQuery
	softDeleteQuery     *tableGenericQuery
	insertOrUpdateQuery *tableGenericQuery
	querySelects        string
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
					offset0, offsetErr := strconv.Atoi(offset)
					if offsetErr != nil {
						err = fmt.Errorf("%s is links, offset is not int", fieldName)
						return
					}
					limit := strings.TrimSpace(setting[ridx+1:])
					limit0, limitErr := strconv.Atoi(limit)
					if limitErr != nil {
						err = fmt.Errorf("%s is links, limit is not int", fieldName)
						return
					}
					col.LinkRange = NewRange(offset0, limit0)
				} else {
					// orders
					col.LinkOrders = make([]*Order, 0, 1)
					orders := strings.Split(setting, " ")
					if len(orders) == 1 {
						col.LinkOrders = append(col.LinkOrders, Asc(setting))
					} else {
						orderField := orders[0]
						orderKind := ""
						for i := 1; i < len(orders); i++ {
							orderKind0 := orders[i]
							if orderKind0 == "" {
								continue
							}
							orderKind = strings.ToUpper(orderKind0)
							break
						}
						if orderKind == "" || orderKind == "ASC" {
							col.LinkOrders = append(col.LinkOrders, Asc(orderField))
						} else {
							col.LinkOrders = append(col.LinkOrders, Desc(orderField))
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

func (t *table) TableName() (v string) {
	v = `"` + t.Name + `"`
	return
}

func (t *table) findPk() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.isPk() || c.isIncrPk() {
			v = append(v, c)
		}
	}
	return
}

func (t *table) hasIncrPk() (v bool) {
	for _, c := range t.Columns {
		if c.isIncrPk() {
			v = true
			return
		}
	}
	return
}

func (t *table) findAuditModify() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.isAmb() || c.isAmt() {
			v = append(v, c)
		}
	}
	return
}

func (t *table) findAuditDelete() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.isAdb() || c.isAdt() {
			v = append(v, c)
		}
	}
	return
}

func (t *table) findAuditVersion() (v *column) {
	for _, c := range t.Columns {
		if c.isAol() {
			v = c
			return
		}
	}
	return
}

func (t *table) generateInsertSQL() (query string, columns []*column) {
	columns = make([]*column, 0, 1)
	idx := 0
	pks := ``
	query = `INSERT INTO ` + t.fullName() + ` `
	cols := ``
	values := ``
	for _, c := range t.Columns {
		if c.isIncrPk() || c.isAmb() || c.isAmt() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
			continue
		}
		if c.isPk() {
			pks = pks + ", " + c.queryName()
		}
		cols = cols + `, ` + c.queryName()
		if c.isAol() {
			values = values + `, 1`
			continue
		}
		idx++
		values = values + `, ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, c)
	}
	cols = cols[2:]
	values = values[2:]
	query = query + `(` + cols + `)` + ` VALUES (` + values + `)`
	if len(pks) > 0 {
		pks = pks[2:]
		query = query + ` ON CONFLICT (` + pks + `) DO NOTHING`
	}
	return
}

func (t *table) generateInsertWhenExistOrNotSQL(exist bool, sourceSQL string) (query string, columns []*column) {
	columns = make([]*column, 0, 1)
	idx := 0
	query = `INSERT INTO ` + t.fullName() + ` `
	cols := ``
	values := ``
	for _, c := range t.Columns {
		if c.isIncrPk() || c.isAmb() || c.isAmt() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
			continue
		}
		cols = cols + `, ` + c.queryName()
		if c.isAol() {
			values = values + `, 1`
			continue
		}
		idx++
		values = values + `, ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, c)
	}
	cols = cols[2:]
	values = values[2:]
	query = query + `(` + cols + `)` + ` SELECT ` + values + ` FROM (SELECT 1) AS "__TMP" WHERE `
	if exist {
		query = query + `EXISTS`
	} else {
		query = query + `NOT EXISTS`
	}
	query = query + ` (SELECT 1 FROM (` + sourceSQL + `))`
	return
}

func (t *table) generateUpdateSQL() (query string, columns []*column) {
	columns = make([]*column, 0, 1)
	idx := 0
	pks := make([]*column, 0, 1)

	var aol *column
	query = `UPDATE ` + t.fullName() + ` SET `
	set := ``
	for _, c := range t.Columns {
		if c.isPk() || c.isIncrPk() {
			pks = append(pks, c)
			continue
		}
		if c.isAcb() || c.isAct() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
			continue
		}
		if c.isAol() {
			set = set + `, ` + aol.queryName() + ` = ` + aol.queryName() + `+1`
			continue
		}
		idx++
		set = set + `, ` + c.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, c)
	}
	set = set[2:]
	query = query + set + ` WHERE `
	cond := ``
	for _, pk := range pks {
		idx++
		cond = cond + ` AND ` + pk.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, pk)
	}
	if aol != nil {
		idx++
		cond = cond + ` AND ` + aol.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, aol)
	}
	cond = cond[5:]
	query = query + cond
	return
}

func (t *table) generateDeleteSQL() (query string, columns []*column) {

	columns = make([]*column, 0, 1)
	idx := 0
	pks := t.findPk()
	aol := t.findAuditVersion()
	query = `DELETE FROM ` + t.fullName() + ` WHERE `
	cond := ``
	for _, pk := range pks {
		idx++
		cond = cond + ` AND ` + pk.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, pk)
	}
	if aol != nil {
		idx++
		cond = cond + ` AND ` + aol.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, aol)
	}
	cond = cond[5:]
	query = query + cond
	return
}

func (t *table) generateSoftDeleteSQL() (query string, columns []*column) {
	deleteColumns := t.findAuditDelete()
	if len(deleteColumns) == 0 {
		return
	}
	columns = make([]*column, 0, 1)
	idx := 0
	pks := t.findPk()
	aol := t.findAuditVersion()
	query = `UPDATE ` + t.fullName() + ` SET `
	set := ``
	for _, deleteColumn := range deleteColumns {
		idx++
		set = set + `, ` + deleteColumn.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, deleteColumn)
	}
	if aol != nil {
		set = set + `, ` + aol.queryName() + ` = ` + aol.queryName() + `+1`
	}
	set = set[2:]
	query = query + set + ` WHERE `
	cond := ``
	for _, pk := range pks {
		idx++
		cond = cond + ` AND ` + pk.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, pk)
	}
	if aol != nil {
		idx++
		cond = cond + ` AND ` + aol.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, aol)
	}
	cond = cond[5:]
	query = query + cond
	return
}

func (t *table) generateInsertOrUpdateSQL() (query string, columns []*column) {
	if t.hasIncrPk() {
		return
	}
	if len(t.findPk()) == 0 {
		return
	}
	columns = make([]*column, 0, 1)
	idx := 0
	pks := ``
	query = `INSERT INTO ` + t.fullName() + ` `
	cols := ``
	values := ``
	for _, c := range t.Columns {
		if c.isIncrPk() || c.isAmb() || c.isAmt() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
			continue
		}
		if c.isPk() {
			pks = pks + ", " + c.queryName()
		}
		cols = cols + `, ` + c.queryName()
		if c.isAol() {
			values = values + `, 1`
			continue
		}
		idx++
		values = values + `, ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, c)
	}
	cols = cols[2:]
	values = values[2:]
	query = query + `(` + cols + `)` + ` VALUES (` + values + `)`
	if len(pks) > 0 {
		pks = pks[2:]
		query = query + ` ON CONFLICT (` + pks + `) DO `
	}
	// update
	var aol *column
	query = `UPDATE SET `
	set := ``
	for _, c := range t.Columns {
		if c.isPk() || c.isIncrPk() || c.isAcb() || c.isAct() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
			continue
		}
		if c.isAol() {
			set = set + `, ` + aol.queryName() + ` = ` + aol.queryName() + `+1`
			continue
		}
		idx++
		set = set + `, ` + c.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, c)
	}
	set = set[2:]
	query = query + set
	return
}

func (t *table) generateExistSQL(conditions *Conditions) (query string, args *sql.Tuple) {
	query = `SELECT 1 FROM ` + t.fullName()
	if conditions != nil {
		conditionQuery, conditionArgs := conditions.QueryAndArguments()
		query = query + " WHERE " + conditionQuery
		args = conditionArgs
	}
	return
}

func (t *table) generateCountSQL(conditions *Conditions) (query string, args *sql.Tuple) {
	query = `SELECT count(1) FROM ` + t.fullName()
	if conditions != nil {
		conditionQuery, conditionArgs := conditions.QueryAndArguments()
		query = query + " WHERE " + conditionQuery
		args = conditionArgs
	}
	return
}

func (t *table) generateQuerySelects() (selects string) {
	for _, c := range t.Columns {
		selects = selects + ", " + c.generateSelect()
	}
	selects = selects[1:]
	return
}

func (t *table) generateQuerySQL(conditions *Conditions, rng *Range, orders []*Order) (query string, args *sql.Tuple) {
	query = `SELECT ` + t.querySelects + ` FROM ` + t.fullName()
	if conditions != nil {
		conditionQuery, conditionArgs := conditions.QueryAndArguments()
		query = query + " WHERE " + conditionQuery
		args = conditionArgs
	}
	if orders != nil && len(orders) > 0 {
		orderQuery := ""
		for _, order := range orders {
			orderKind := "ASC"
			if order.Desc {
				orderKind = "DESC"
			}
			orderQuery = orderQuery + `, ` + `"` + order.Column + `" ` + orderKind
		}
		orderQuery = orderQuery[1:]
		query = query + ` ORDER BY` + orderQuery
	}
	if rng != nil {
		query = query + ` OFFSET ` + strconv.Itoa(rng.Offset) + ` LIMIT ` + strconv.Itoa(rng.Limit)
	}
	return
}
