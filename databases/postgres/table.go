package postgres

import (
	"fmt"
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

		insertUseQuery, insertQuery, insertColumns := v.generateInsertSQL()
		if insertQuery != "" {
			v.insertQuery = &tableGenericQuery{
				useQuery: insertUseQuery,
				query:    insertQuery,
				columns:  insertColumns,
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
		insertOrUpdateUseQuery, insertOrUpdateQuery, insertOrUpdateColumns := v.generateInsertOrUpdateSQL()
		if insertOrUpdateQuery != "" {
			v.insertOrUpdateQuery = &tableGenericQuery{
				useQuery: insertOrUpdateUseQuery,
				query:    insertOrUpdateQuery,
				columns:  insertOrUpdateColumns,
			}
		}
		insertWhenExistOrNotUseQuery, insertWhenExistOrNotQuery, insertWhenExistOrNotColumns := v.generateInsertWhenExistOrNotSQL()
		if insertWhenExistOrNotQuery != "" {
			v.insertWhenExistOrNotQuery = &tableGenericQuery{
				useQuery: insertWhenExistOrNotUseQuery,
				query:    insertWhenExistOrNotQuery,
				columns:  insertWhenExistOrNotColumns,
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
	useQuery bool
	query    string
	columns  []*column
}

type table struct {
	Schema                    string
	Name                      string
	Columns                   []*column
	insertQuery               *tableGenericQuery
	insertOrUpdateQuery       *tableGenericQuery
	insertWhenExistOrNotQuery *tableGenericQuery
	updateQuery               *tableGenericQuery
	deleteQuery               *tableGenericQuery
	softDeleteQuery           *tableGenericQuery
	querySelects              string
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
		t.Columns = append(t.Columns, newColumn(t, normalCol, false, columnName, fieldName))
		return
	}
	kind := strings.ToLower(strings.TrimSpace(tagItems[1]))
	conflict := strings.Contains(kind, conflictCol)
	if conflict {
		if plusIdx := strings.Index(kind, "+"); plusIdx > 0 {
			kind = kind[0:plusIdx]
		} else {
			kind = normalCol
		}
	}
	switch kind {
	case pkCol:
		t.Columns = append(t.Columns, newColumn(t, pkCol, conflict, columnName, fieldName))
	case incrPkCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(0)) {
			err = fmt.Errorf("%s is incr pk, type must be int64", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, incrPkCol, conflict, columnName, fieldName))
	case normalCol:
		t.Columns = append(t.Columns, newColumn(t, normalCol, conflict, columnName, fieldName))
	case jsonCol:
		t.Columns = append(t.Columns, newColumn(t, jsonCol, conflict, columnName, fieldName))
	case auditCreateByCol:
		if !(field.Type.ConvertibleTo(reflect.TypeOf("")) || field.Type.ConvertibleTo(reflect.TypeOf(int64(0)))) {
			err = fmt.Errorf("%s is audit create by, type must be int64 or string", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditCreateByCol, conflict, columnName, fieldName))
	case auditCreateAtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s is audit create at, type must be time.Time", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditCreateAtCol, conflict, columnName, fieldName))
	case auditModifyBtCol:
		if !(field.Type.ConvertibleTo(reflect.TypeOf("")) || field.Type.ConvertibleTo(reflect.TypeOf(int64(0)))) {
			err = fmt.Errorf("%s is audit modify by, type must be int64 or string", fieldName)
			return
		}
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit modify by, type must be string", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditModifyBtCol, conflict, columnName, fieldName))
	case auditModifyAtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s is audit modify at, type must be time.Time", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditModifyAtCol, conflict, columnName, fieldName))
	case auditDeleteByCol:
		if !(field.Type.ConvertibleTo(reflect.TypeOf("")) || field.Type.ConvertibleTo(reflect.TypeOf(int64(0)))) {
			err = fmt.Errorf("%s is audit delete by, type must be int64 or string", fieldName)
			return
		}
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit delete by, type must be string", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditDeleteByCol, conflict, columnName, fieldName))
	case auditDeleteAtCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s is audit delete at, type must be time.Time", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditDeleteAtCol, conflict, columnName, fieldName))
	case auditVersionCol:
		if !field.Type.ConvertibleTo(reflect.TypeOf("")) {
			err = fmt.Errorf("%s is audit version, type must be int64", fieldName)
			return
		}
		t.Columns = append(t.Columns, newColumn(t, auditVersionCol, conflict, columnName, fieldName))
	case virtualCol:
		if len(tagItems) < 3 {
			err = fmt.Errorf("%s is vc, source sql must be setted", fieldName)
			return
		}
		sourceSQL := strings.TrimSpace(tagItems[2])
		col := newColumn(t, virtualCol, conflict, columnName, fieldName)
		col.VirtualQuery = sourceSQL
		t.Columns = append(t.Columns, col)
	case refCol:
		if len(tagItems) != 3 {
			err = fmt.Errorf("%s is ref, ref refenerce must be setted", fieldName)
			return
		}
		refs := strings.Split(tagItems[2], "+")
		if len(refs) != 2 {
			err = fmt.Errorf("%s is ref, ref refenerce must be host 'host table column + ref target column'", fieldName)
			return
		}
		hostRefColumnName := strings.TrimSpace(refs[0])
		targetRefColumnName := strings.TrimSpace(refs[1])
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
		refTable := createOrLoadTable(reflect.New(refType).Interface())
		targetRefColumn, hasTargetLinkColumn := refTable.findColumn(targetRefColumnName)
		if !hasTargetLinkColumn {
			err = fmt.Errorf("%s is ref, %s ref column of ref refenerce was not found", fieldName, targetRefColumnName)
			return
		}
		col := newColumn(t, refCol, conflict, hostRefColumnName, fieldName)
		col.Ref = refTable
		col.RefName = columnName
		col.RefTargetColumn = targetRefColumn
		t.Columns = append(t.Columns, col)
	case linkCol:
		if len(tagItems) != 3 {
			err = fmt.Errorf("%s is link, link refenerce must be setted", fieldName)
			return
		}
		linkRef := strings.Split(tagItems[2], "+")
		if len(linkRef) != 2 {
			err = fmt.Errorf("%s is link, link refenerce must be host 'host table column + link target column'", fieldName)
			return
		}
		hostLinkColumnName := strings.TrimSpace(linkRef[0])
		hostLinkColumn, hasHostLinkColumn := t.findColumn(hostLinkColumnName)
		if !hasHostLinkColumn {
			err = fmt.Errorf("%s is link, host link column of link refenerce must be setted and on top of %s ", fieldName, fieldName)
			return
		}
		targetLinkColumnName := strings.TrimSpace(linkRef[1])
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
		linkTable := createOrLoadTable(reflect.New(linkType).Interface())
		targetLinkColumn, hasTargetLinkColumn := linkTable.findColumn(targetLinkColumnName)
		if !hasTargetLinkColumn {
			err = fmt.Errorf("%s is link, %s link column of link refenerce was not found", fieldName, targetLinkColumnName)
			return
		}
		col := newColumn(t, linkCol, conflict, columnName, fieldName)
		col.Link = linkTable
		col.LinkHostColumn = hostLinkColumn
		col.LinkTargetColumn = targetLinkColumn
		t.Columns = append(t.Columns, col)
	case linksCol:
		fieldType := field.Type
		if !(fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Array) {
			err = fmt.Errorf("%s is links, field type must slice point struct", fieldName)
			return
		}
		if len(tagItems) < 3 {
			err = fmt.Errorf("%s is link, link refenerce must be setted", fieldName)
			return
		}
		linkRef := strings.Split(tagItems[2], "+")
		if len(linkRef) != 2 {
			err = fmt.Errorf("%s is links, links refenerce must be host 'host table column + link target column'", fieldName)
			return
		}
		hostLinkColumnName := strings.TrimSpace(linkRef[0])
		hostLinkColumn, hasHostLinkColumn := t.findColumn(hostLinkColumnName)
		if !hasHostLinkColumn {
			err = fmt.Errorf("%s is links, host link column of links refenerce must be setted and on top of %s ", fieldName, fieldName)
			return
		}
		targetLinkColumnName := strings.TrimSpace(linkRef[1])
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
		linkTable := createOrLoadTable(reflect.New(linkType).Interface())
		targetLinkColumn, hasTargetLinkColumn := linkTable.findColumn(targetLinkColumnName)
		if !hasTargetLinkColumn {
			err = fmt.Errorf("%s is link, target %s column of link refenerce was not found", fieldName, targetLinkColumnName)
			return
		}
		col := newColumn(t, linksCol, conflict, columnName, fieldName)
		col.Link = linkTable
		col.LinkHostColumn = hostLinkColumn
		col.LinkTargetColumn = targetLinkColumn
		if len(tagItems) > 3 {
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

func (t *table) findColumn(name string) (c *column, has bool) {
	for _, col := range t.Columns {
		if col.Name == name {
			c = col
			has = true
			return
		}
	}
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

func (t *table) findAuditCreate() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.isAcb() || c.isAct() {
			v = append(v, c)
			if len(v) == 2 {
				return
			}
		}
	}
	return
}

func (t *table) findAuditModify() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.isAmb() || c.isAmt() {
			v = append(v, c)
			if len(v) == 2 {
				return
			}
		}
	}
	return
}

func (t *table) findAuditDelete() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.isAdb() || c.isAdt() {
			v = append(v, c)
			if len(v) == 2 {
				return
			}
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

func (t *table) findConflicts() (v []*column) {
	v = make([]*column, 0, 1)
	for _, c := range t.Columns {
		if c.Conflict {
			v = append(v, c)
		}
	}
	return
}

func (t *table) generateInsertSQL() (useQuery bool, query string, columns []*column) {
	columns = make([]*column, 0, 1)
	idx := 0
	incrPkName := ""
	query = `INSERT INTO ` + t.fullName() + ` `
	cols := ``
	values := ``
	for _, c := range t.Columns {
		if c.isIncrPk() {
			incrPkName = c.queryName()
			useQuery = true
			continue
		}
		if c.isAmb() || c.isAmt() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
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
	query = query + `(` + cols + `)` + ` VALUES (` + values + `)`
	//conflicts
	conflicts := ""
	pks := t.findPk()
	if len(pks) > 0 {
		for _, pk := range pks {
			if pk.isIncrPk() {
				continue
			}
			conflicts = conflicts + ", " + pk.queryName()
		}
	}

	conflictColumns := t.findConflicts()
	if len(conflictColumns) > 0 {
		for _, conflictColumn := range conflictColumns {
			conflicts = conflicts + ", " + conflictColumn.queryName()
		}
	}
	if conflicts != "" {
		conflicts = conflicts[2:]
		query = query + ` ON CONFLICT (` + conflicts + `) DO NOTHING`
	}
	if useQuery {
		query = query + ` RETURNING ` + incrPkName + ` AS "LAST_INSERT_ID"`
	}
	return
}

func (t *table) generateInsertWhenExistOrNotSQL() (useQuery bool, query string, columns []*column) {
	columns = make([]*column, 0, 1)
	idx := 0
	incrPkName := ""
	query = `INSERT INTO ` + t.fullName() + ` `
	cols := ``
	values := ``
	for _, c := range t.Columns {
		if c.isIncrPk() {
			incrPkName = c.queryName()
			useQuery = true
			continue
		}
		if c.isAmb() || c.isAmt() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
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
	query = query + `(` + cols + `)` + ` SELECT ` + values + ` FROM (SELECT 1) AS "__TMP__" WHERE `
	query = query + `$$EXISTS$$`
	query = query + ` (SELECT 1 FROM (` + "$$SOURCE_QUERY$$" + `) AS "__SRC__")`
	if useQuery {
		query = query + ` RETURNING ` + incrPkName + ` AS "LAST_INSERT_ID"`
	}
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
			aol = c
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

func (t *table) generateInsertOrUpdateSQL() (useQuery bool, query string, columns []*column) {
	//conflicts
	conflicts := ""
	pks := t.findPk()
	if len(pks) > 0 {
		for _, pk := range pks {
			if pk.isIncrPk() {
				continue
			}
			conflicts = conflicts + ", " + pk.queryName()
		}
	}
	conflictColumns := t.findConflicts()
	if len(conflictColumns) > 0 {
		for _, conflictColumn := range conflictColumns {
			conflicts = conflicts + ", " + conflictColumn.queryName()
		}
	}
	if conflicts == "" {
		return
	}
	conflicts = conflicts[2:]
	columns = make([]*column, 0, 1)
	incrPkName := ""
	idx := 0
	query = `INSERT INTO ` + t.fullName() + ` `
	cols := ``
	values := ``
	for _, c := range t.Columns {
		if c.isIncrPk() {
			incrPkName = c.queryName()
			useQuery = true
		}
		if c.isAmb() || c.isAmt() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
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
	query = query + `(` + cols + `)` + ` VALUES (` + values + `)`
	query = query + ` ON CONFLICT (` + conflicts + `) DO `
	// update
	var aol *column
	query = query + `UPDATE SET `
	set := ``
	for _, c := range t.Columns {
		if c.isPk() || c.isIncrPk() || c.isAcb() || c.isAct() || c.isAdb() || c.isAdt() || c.isVc() || c.isLink() || c.isLinks() {
			continue
		}
		if c.isAol() {
			aol = c
			set = set + `, ` + aol.queryName() + ` = ` + aol.queryName() + `+1`
			continue
		}
		idx++
		set = set + `, ` + c.queryName() + ` = ` + fmt.Sprintf("$%d", idx)
		columns = append(columns, c)
	}
	set = set[2:]
	query = query + set
	if useQuery {
		query = query + ` RETURNING ` + incrPkName + ` AS "LAST_INSERT_ID" `
	}
	return
}

func (t *table) generateExistSQL(conditions *Conditions) (query string, args []interface{}) {
	cc := ""
	pks := t.findPk()
	if len(pks) > 0 {
		cc = pks[0].queryName()
	} else {
		cc = "1"
	}
	query = `SELECT ` + cc + ` AS "_EXIST_" FROM ` + t.fullName()
	if conditions != nil {
		conditionQuery, conditionArgs := conditions.QueryAndArguments()
		query = query + " WHERE " + conditionQuery
		args = conditionArgs
	}
	return
}

func (t *table) generateCountSQL(conditions *Conditions) (query string, args []interface{}) {
	cc := ""
	pks := t.findPk()
	if len(pks) > 0 {
		cc = pks[0].queryName()
	} else {
		cc = "1"
	}
	query = `SELECT count(` + cc + `) AS "_COUNT_" FROM ` + t.fullName()
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
	selects = selects[2:]
	return
}

func (t *table) generateQuerySQL(conditions *Conditions, rng *Range, orders []*Order) (query string, args []interface{}) {
	pks := t.findPk()
	if len(pks) > 0 {
		pk := pks[0].queryName()
		alias := fmt.Sprintf("\"_%s_\"", t.Name)
		orderQuery := ""
		rngQuery := ""
		innerQuery := `SELECT ` + pk + ` FROM ` + t.fullName()
		if conditions != nil {
			conditionQuery, conditionArgs := conditions.QueryAndArguments()
			innerQuery = innerQuery + " WHERE " + conditionQuery
			args = conditionArgs
		}
		if orders != nil && len(orders) > 0 {
			for _, order := range orders {
				orderKind := "ASC"
				if order.Desc {
					orderKind = "DESC"
				}
				orderQuery = orderQuery + `, ` + `"` + order.Column + `" ` + orderKind
			}
			orderQuery = orderQuery[1:]
			innerQuery = innerQuery + ` ORDER BY` + orderQuery
		}
		if rng != nil {
			rngQuery = ` OFFSET ` + strconv.Itoa(rng.Offset) + ` LIMIT ` + strconv.Itoa(rng.Limit)
			innerQuery = innerQuery + rngQuery
		}
		query = `SELECT ` + t.querySelects + ` FROM ` + t.fullName()
		query = query + ` INNER JOIN (` + innerQuery + `) AS ` + alias + ` ON ` + t.fullName() + `.` + pk + ` = ` + alias + `.` + pk
		if orderQuery != "" {
			query = query + ` ORDER BY` + orderQuery
		}
		//if rngQuery != "" {
		//	query = query + rngQuery
		//}
	} else {
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
	}
	return
}
