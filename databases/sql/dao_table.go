package sql

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	columnTagPk       = "PK"
	columnTagCreateBY = "ACB"
	columnTagCreateAT = "ACT"
	columnTagModifyBY = "AMB"
	columnTagModifyAT = "AMT"
	columnTagDeleteBY = "ADB"
	columnTagDeleteAT = "ADT"
	columnTagVersion  = "OL"
	columnTagVirtual  = "VC"
	columnTagFk       = "FK"
	columnTagLk       = "LK"
	columnTagJson     = "JSON"
)

const (
	refTag        = "ref"
	sortTag       = "sort"
	virtualSrcTag = "src"
)

// +-------------------------------------------------------------------------------------------------------------------+

func newTableInfo(v interface{}, driver string) (info *tableInfo) {
	rt := reflect.TypeOf(v).Elem()
	key := fmt.Sprintf("%s:%s", rt.PkgPath(), rt.Name())
	cached, hasCache := tableInfoMap.Load(key)
	if hasCache {
		info = cached.(*tableInfo)
		return
	}
	table, convertOk := v.(TableRow)
	if !convertOk {
		panic(fmt.Sprintf("fns SQL: use DAO failed for %s/%s is not TableRow implement", rt.PkgPath(), rt.Name()))
	}
	namespace, name, alias := table.Table()
	if name == "" {
		panic(fmt.Sprintf("fns SQL: use DAO failed for no table name, %s/%s", rt.PkgPath(), rt.Name()))
	}
	if alias == "" {
		panic(fmt.Sprintf("fns SQL: use DAO failed for no table name alias, %s/%s", rt.PkgPath(), rt.Name()))
	}
	namespace = strings.ToUpper(strings.TrimSpace(namespace))
	name = strings.ToUpper(strings.TrimSpace(name))
	alias = strings.ToUpper(strings.TrimSpace(alias))
	if driver == "postgres" {
		namespace = fmt.Sprintf("\"%s\"", namespace)
		name = fmt.Sprintf("\"%s\"", name)
		alias = fmt.Sprintf("\"%s\"", alias)
	}
	info = &tableInfo{
		Driver:         driver,
		Namespace:      namespace,
		Name:           name,
		Alias:          alias,
		Pks:            make([]*columnInfo, 0, 1),
		CreateBY:       nil,
		CreateAT:       nil,
		ModifyBY:       nil,
		ModifyAT:       nil,
		DeleteBY:       nil,
		DeleteAT:       nil,
		Version:        nil,
		Columns:        make([]*columnInfo, 0, 1),
		ForeignColumns: make([]*foreignColumnInfo, 0, 1),
		LinkColumns:    make([]*linkColumnInfo, 0, 1),
		VirtualColumns: make([]*virtualColumnInfo, 0, 1),
		InsertQuery:    queryInfo{},
		UpdateQuery:    queryInfo{},
		DeleteQuery:    queryInfo{},
		GetQuery:       queryInfo{},
		ExistQuery:     queryInfo{},
	}
	fieldNum := rt.NumField()
	for i := 0; i < fieldNum; i++ {
		field := rt.Field(i)
		tag, hasTag := field.Tag.Lookup(columnStructTag)
		if !hasTag {
			continue
		}
		if !strings.Contains(tag, ",") {
			// col
			columnName := strings.ToUpper(strings.TrimSpace(tag))
			if driver == "postgres" {
				columnName = fmt.Sprintf("\"%s\"", columnName)
			}
			column := &columnInfo{
				Name:            columnName,
				Type:            field.Type,
				StructFieldName: field.Name,
			}
			info.Columns = append(info.Columns, column)
			continue
		}
		columnName := tag[0:strings.Index(tag, ",")]
		if driver == "postgres" {
			columnName = fmt.Sprintf("\"%s\"", columnName)
		}
		define := tag[strings.Index(tag, ",")+1:]
		defineOp := ""
		defineOpIdx := strings.Index(define, ":")
		if defineOpIdx > 0 {
			defineOp = define[defineOpIdx+1:]
			define = define[0:defineOpIdx]
		}
		if columnName != "" {
			// JSON
			if define == columnTagJson {
				column := &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
					IsJson:          true,
				}
				info.Columns = append(info.Columns, column)
				continue
			}
			// pk
			if define == columnTagPk {
				pk := &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				info.Pks = append(info.Pks, pk)
				continue
			}
			// create
			if define == columnTagCreateBY {
				info.CreateBY = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			if define == columnTagCreateAT {
				if !(field.Type == sqlNullTimeType || field.Type == sqlTimeType) {
					panic(fmt.Sprintf("fns SQL: use DAO failed for CREATE_AT must be time.Time or sql.NullTime, %s/%s", rt.PkgPath(), rt.Name()))
				}
				info.CreateAT = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			// modify
			if define == columnTagModifyBY {
				info.ModifyBY = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			if define == columnTagModifyAT {
				if !(field.Type == sqlNullTimeType || field.Type == sqlTimeType) {
					panic(fmt.Sprintf("fns SQL: use DAO failed for MODIFY_AT must be time.Time or sql.NullTime, %s/%s", rt.PkgPath(), rt.Name()))
				}
				info.ModifyAT = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			// delete
			if define == columnTagDeleteBY {
				info.DeleteBY = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			if define == columnTagDeleteAT {
				if !(field.Type == sqlNullTimeType || field.Type == sqlTimeType) {
					panic(fmt.Sprintf("fns SQL: use DAO failed for DELETE_AT must be time.Time or sql.NullTime, %s/%s", rt.PkgPath(), rt.Name()))
				}
				info.DeleteAT = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			// version
			if define == columnTagVersion {
				if !(field.Type == sqlNullInt16Type || field.Type == sqlNullInt32Type || field.Type == sqlNullInt64Type || field.Type == sqlIntType || field.Type == sqlInt64Type) {
					panic(fmt.Sprintf("fns SQL: use DAO failed for VERSION must be int, int64 or sql.NullInt64, %s/%s", rt.PkgPath(), rt.Name()))
				}
				info.Version = &columnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
				}
				continue
			}
			// FK
			if define == columnTagFk {
				if field.Type.Kind() != reflect.Ptr {
					panic(fmt.Sprintf("fns SQL: use DAO failed for FK must be ptr of struct, %s/%s", rt.PkgPath(), rt.Name()))
				}
				if field.Type.Elem().Kind() != reflect.Struct {
					panic(fmt.Sprintf("fns SQL: use DAO failed for FK must be ptr of struct, %s/%s", rt.PkgPath(), rt.Name()))
				}
				info.ForeignColumns = append(info.ForeignColumns, &foreignColumnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
					Sync:            defineOp == "SYNC",
				})
				continue
			}
			// LK
			if define == columnTagLk {
				if field.Type.Kind() != reflect.Slice && field.Type.Kind() != reflect.Array {
					panic(fmt.Sprintf("fns SQL: use DAO failed for LK must be slice and element must be ptr of stuct, %s/%s", rt.PkgPath(), rt.Name()))
				}
				if field.Type.Elem().Kind() != reflect.Ptr {
					panic(fmt.Sprintf("fns SQL: use DAO failed for LK must be slice and element must be ptr of stuct, %s/%s", rt.PkgPath(), rt.Name()))
				}
				if field.Type.Elem().Elem().Kind() != reflect.Struct {
					panic(fmt.Sprintf("fns SQL: use DAO failed for LK must be slice and element must be ptr of stuct, %s/%s", rt.PkgPath(), rt.Name()))
				}
				ref, hasRef := field.Tag.Lookup(refTag)
				refLeftColumn := ""
				refRightColumn := ""
				if hasRef {
					ref = strings.ToUpper(strings.TrimSpace(ref))
					refs := strings.Split(ref, ",")
					if len(refs) != 2 {
						panic(fmt.Sprintf("fns SQL: use DAO failed for LK Tag must define link columns, %s/%s", rt.PkgPath(), rt.Name()))
					}
					refLeftColumn = mapRelationName(strings.TrimSpace(refs[0]))
					refRightColumn = mapRelationName(strings.TrimSpace(refs[1]))
				}
				sort, hasSort := field.Tag.Lookup(sortTag)
				if hasSort {
					sort = strings.ToUpper(strings.TrimSpace(sort))
					if driver == "postgres" {
						x := ""
						sortItems := strings.Split(sort, ",")
						for j, item := range sortItems {
							item = strings.TrimSpace(item)
							colIdx := strings.Index(item, " ")
							if j == 0 {
								if colIdx > 0 {
									x = fmt.Sprintf("\"%s\"", item[0:colIdx]) + item[colIdx:]
								} else {
									x = fmt.Sprintf("\"%s\"", item)
								}
							} else {
								if colIdx > 0 {
									x = x + "," + fmt.Sprintf("\"%s\"", item[0:colIdx]) + item[colIdx:]
								} else {
									x = x + "," + fmt.Sprintf("\"%s\"", item)
								}
							}
						}
						sort = x
					}
				}

				info.LinkColumns = append(info.LinkColumns, &linkColumnInfo{
					Sync:            defineOp == "SYNC",
					LeftColumn:      refLeftColumn,
					RightColumn:     refRightColumn,
					OrderBy:         sort,
					SliceType:       field.Type,
					ElementType:     field.Type.Elem(),
					StructFieldName: field.Name,
				})
				continue
			}
			if define == columnTagVirtual {
				src, hasSrc := field.Tag.Lookup(virtualSrcTag)
				if !hasSrc {
					panic(fmt.Sprintf("fns SQL: use DAO failed for virtual column must has src tag, %s/%s", rt.PkgPath(), rt.Name()))
				}
				info.VirtualColumns = append(info.VirtualColumns, &virtualColumnInfo{
					Name:            columnName,
					Type:            field.Type,
					StructFieldName: field.Name,
					Source:          src,
				})
				continue
			}
		}
	}
	info.genExistQuery()
	info.genGetQuery()
	info.genInsert()
	info.genUpdate()
	info.genDelete()
	tableInfoMap.Store(key, info)
	return
}

type tableInfo struct {
	Driver         string
	Namespace      string
	Name           string
	Alias          string
	Pks            []*columnInfo
	CreateBY       *columnInfo
	CreateAT       *columnInfo
	ModifyBY       *columnInfo
	ModifyAT       *columnInfo
	DeleteBY       *columnInfo
	DeleteAT       *columnInfo
	Version        *columnInfo
	Columns        []*columnInfo
	ForeignColumns []*foreignColumnInfo
	LinkColumns    []*linkColumnInfo
	VirtualColumns []*virtualColumnInfo
	InsertQuery    queryInfo
	UpdateQuery    queryInfo
	DeleteQuery    queryInfo
	GetQuery       queryInfo
	ExistQuery     queryInfo
}

func (info *tableInfo) IsJson(fieldName string) (ok bool) {
	for _, column := range info.Columns {
		if column.StructFieldName == fieldName {
			if column.IsJson {
				ok = true
				return
			}
		}
	}
	return
}

func (info *tableInfo) GetColumnField(columnName string) (name string) {
	for _, pk := range info.Pks {
		if pk.Name == columnName {
			name = pk.StructFieldName
			return
		}
	}
	for _, column := range info.Columns {
		if column.Name == columnName {
			name = column.StructFieldName
			return
		}
	}
	for _, column := range info.ForeignColumns {
		if column.Name == columnName {
			name = column.StructFieldName
			return
		}
	}
	return
}

func (info *tableInfo) GetForeign(fieldName string) (v *foreignColumnInfo) {
	for _, column := range info.ForeignColumns {
		if column.StructFieldName == fieldName {
			v = column
		}
	}
	return
}

func (info *tableInfo) IsForeign(fieldName string) (ok bool) {
	for _, column := range info.ForeignColumns {
		ok = column.StructFieldName == fieldName
		if ok {
			return
		}
	}
	return
}

func (info *tableInfo) GetLink(fieldName string) (v *linkColumnInfo) {
	for _, column := range info.LinkColumns {
		if column.StructFieldName == fieldName {
			v = column
		}
	}
	return
}

func (info *tableInfo) IsLink(fieldName string) (ok bool) {
	for _, column := range info.LinkColumns {
		ok = column.StructFieldName == fieldName
		if ok {
			return
		}
	}
	return
}

func (info *tableInfo) IsVirtual(fieldName string) (ok bool) {
	for _, column := range info.VirtualColumns {
		ok = column.StructFieldName == fieldName
		if ok {
			return
		}
	}
	return
}

func (info *tableInfo) genExistQuery() {
	query := "SELECT 1 AS " + info.Alias + " FROM "
	if info.Namespace != "" {
		query = query + info.Namespace + "." + info.Name
	} else {
		query = query + info.Name + " AS "
	}
	params := make([]string, 0, 1)
	query = query + " WHERE "
	for i, pk := range info.Pks {
		if i == 0 {
			if info.Driver == "postgres" {
				query = query + pk.Name + fmt.Sprintf("=$%d", i+1)
			} else {
				query = query + pk.Name + "=?"
			}
		} else {
			if info.Driver == "postgres" {
				query = query + "AND " + pk.Name + fmt.Sprintf("=$%d", i+1)
			} else {
				query = query + "AND " + pk.Name + "=?"
			}
		}
		params = append(params, pk.StructFieldName)
	}
	info.ExistQuery.Query = query
	info.ExistQuery.Params = params
}

func (info *tableInfo) genGetQuery() {
	query := "SELECT "
	selects := ""
	// pk
	for _, pk := range info.Pks {
		selects = selects + ", " + info.Alias + "." + pk.Name
	}
	// audit
	if info.CreateBY != nil {
		selects = selects + ", " + info.Alias + "." + info.CreateBY.Name
	}
	if info.CreateAT != nil {
		selects = selects + ", " + info.Alias + "." + info.CreateAT.Name
	}
	if info.ModifyBY != nil {
		selects = selects + ", " + info.Alias + "." + info.ModifyBY.Name
	}
	if info.ModifyAT != nil {
		selects = selects + ", " + info.Alias + "." + info.ModifyAT.Name
	}
	if info.DeleteBY != nil {
		selects = selects + ", " + info.Alias + "." + info.DeleteBY.Name
	}
	if info.DeleteAT != nil {
		selects = selects + ", " + info.Alias + "." + info.DeleteAT.Name
	}
	if info.Version != nil {
		selects = selects + ", " + info.Alias + "." + info.Version.Name
	}
	// col
	for _, column := range info.Columns {
		selects = selects + ", " + info.Alias + "." + column.Name
	}
	// fk
	for _, column := range info.ForeignColumns {
		selects = selects + ", " + info.Alias + "." + column.Name
	}
	// vc
	for _, column := range info.VirtualColumns {
		selects = selects + ", (" + column.Source + ") AS " + column.Name
	}

	query = query + selects[1:]
	if info.Namespace != "" {
		query = query + " FROM " + info.Namespace + "." + info.Name + " AS " + info.Alias
	} else {
		query = query + " FROM " + info.Name + " AS " + info.Alias
	}
	params := make([]string, 0, 1)
	query = query + " WHERE "
	for i, pk := range info.Pks {
		if i == 0 {
			if info.Driver == "postgres" {
				query = query + info.Alias + "." + pk.Name + fmt.Sprintf("=$%d", i+1)
			} else {
				query = query + info.Alias + "." + pk.Name + "=?"
			}
		} else {
			if info.Driver == "postgres" {
				query = query + "AND " + info.Alias + "." + pk.Name + fmt.Sprintf("=$%d", i+1)
			} else {
				query = query + "AND " + info.Alias + "." + pk.Name + "=?"
			}
		}
		params = append(params, pk.StructFieldName)
	}
	info.GetQuery.Query = query
	info.GetQuery.Params = params
}

func (info *tableInfo) genLinkQuery(link *linkColumnInfo) (query string) {
	query = "SELECT "
	selects := ""
	// pk
	for _, pk := range info.Pks {
		selects = selects + ", " + info.Alias + "." + pk.Name
	}
	// audit
	if info.CreateBY != nil {
		selects = selects + ", " + info.Alias + "." + info.CreateBY.Name
	}
	if info.CreateAT != nil {
		selects = selects + ", " + info.Alias + "." + info.CreateAT.Name
	}
	if info.ModifyBY != nil {
		selects = selects + ", " + info.Alias + "." + info.ModifyBY.Name
	}
	if info.ModifyAT != nil {
		selects = selects + ", " + info.Alias + "." + info.ModifyAT.Name
	}
	if info.DeleteBY != nil {
		selects = selects + ", " + info.Alias + "." + info.DeleteBY.Name
	}
	if info.DeleteAT != nil {
		selects = selects + ", " + info.Alias + "." + info.DeleteAT.Name
	}
	if info.Version != nil {
		selects = selects + ", " + info.Alias + "." + info.Version.Name
	}
	// vc
	for _, column := range info.VirtualColumns {
		selects = selects + ", (" + column.Source + ") AS " + column.Name
	}

	query = query + selects[1:]
	if info.Namespace != "" {
		query = query + " FROM " + info.Namespace + "." + info.Name + " AS " + info.Alias
	} else {
		query = query + " FROM " + info.Name + " AS " + info.Alias
	}
	query = query + " WHERE "

	if info.Driver == "postgres" {
		query = query + info.Alias + "." + link.RightColumn + "=$1"
	} else {
		query = query + info.Alias + "." + link.RightColumn + "=?"
	}

	if link.OrderBy != "" {
		query = query + " ORDER BY " + link.OrderBy
	}
	return
}

// genInsert
// insert ... ON CONFLICT (pk) DO NOTHING http://www.postgres.cn/docs/13/sql-insert.html
// insert ... ON DUPLICATE KEY UPDATE ... https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html
func (info *tableInfo) genInsert() {
	query := "INSERT INTO "
	if info.Namespace != "" {
		query = query + info.Namespace + "." + info.Name
	} else {
		query = query + info.Name
	}
	params := make([]string, 0, 1)
	argIdx := 0
	args := ""
	query = query + " ("
	pks := ""
	for i, pk := range info.Pks {
		if i == 0 {
			pks = pks + pk.Name
		} else {
			pks = pks + ", " + pk.Name
		}
		argIdx++
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("$%d", argIdx)
		} else {
			args = args + ", ?"
		}
		params = append(params, pk.StructFieldName)
	}
	query = query + pks
	if info.CreateBY != nil {
		argIdx++
		query = query + ", " + info.CreateBY.Name
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("$%d", argIdx)
		} else {
			args = args + ", ?"
		}
		params = append(params, info.CreateBY.StructFieldName)
	}
	if info.CreateAT != nil {
		argIdx++
		query = query + ", " + info.CreateAT.Name
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("$%d", argIdx)
		} else {
			args = args + ", ?"
		}
		params = append(params, info.CreateAT.StructFieldName)
	}
	if info.Version != nil {
		argIdx++
		query = query + ", " + info.Version.Name
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("$%d", argIdx)
		} else {
			args = args + ", ?"
		}
		params = append(params, info.Version.StructFieldName)
	}
	for _, column := range info.Columns {
		argIdx++
		query = query + ", " + column.Name
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("$%d", argIdx)
		} else {
			args = args + ", ?"
		}
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		argIdx++
		query = query + ", " + column.Name
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("$%d", argIdx)
		} else {
			args = args + ", ?"
		}
		params = append(params, column.StructFieldName)
	}
	query = query + ") VALUES (" + args[2:] + ")"
	if info.Driver == "postgres" {
		query = query + " ON CONFLICT (" + pks + ") DO NOTHING"
	} else if info.Driver == "mysql" {
		query = query + " ON DUPLICATE KEY UPDATE " + info.Columns[0].Name + " = " + info.Columns[0].Name
	}
	info.InsertQuery.Query = query
	info.InsertQuery.Params = params
}

func (info *tableInfo) genUpdate() {
	query := "UPDATE "
	if info.Namespace != "" {
		query = query + info.Namespace + "." + info.Name
	} else {
		query = query + info.Name
	}
	query = query + " SET "
	argIdx := 0
	args := ""
	params := make([]string, 0, 1)
	if info.ModifyBY != nil {
		argIdx++
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("%s=$%d", info.ModifyBY.Name, argIdx)
		} else {
			args = args + ", " + fmt.Sprintf("%s=?", info.ModifyBY.Name)
		}
		params = append(params, info.ModifyBY.StructFieldName)
	}
	if info.ModifyAT != nil {
		argIdx++
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("%s=$%d", info.ModifyAT.Name, argIdx)
		} else {
			args = args + ", " + fmt.Sprintf("%s=?", info.ModifyAT.Name)
		}
		params = append(params, info.ModifyAT.StructFieldName)
	}
	if info.Version != nil {
		args = args + ", " + fmt.Sprintf("%s=%s+1", info.Version.Name, info.Version.Name)
	}
	for _, column := range info.Columns {
		argIdx++
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("%s=$%d", column.Name, argIdx)
		} else {
			args = args + ", " + fmt.Sprintf("%s=?", column.Name)
		}
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		argIdx++
		if info.Driver == "postgres" {
			args = args + ", " + fmt.Sprintf("%s=$%d", column.Name, argIdx)
		} else {
			args = args + ", " + fmt.Sprintf("%s=?", column.Name)
		}
		params = append(params, column.StructFieldName)
	}
	query = query + args[2:] + " WHERE "
	condition := ""
	for _, column := range info.Pks {
		argIdx++
		if info.Driver == "postgres" {
			condition = condition + " AND " + fmt.Sprintf("%s=$%d", column.Name, argIdx)
		} else {
			condition = condition + " AND " + fmt.Sprintf("%s=?", column.Name)
		}
		params = append(params, column.StructFieldName)
	}
	if info.Version != nil {
		argIdx++
		if info.Driver == "postgres" {
			condition = condition + " AND " + fmt.Sprintf("%s=$%d", info.Version.Name, argIdx)
		} else {
			condition = condition + " AND " + fmt.Sprintf("%s=?", info.Version.Name)
		}
		params = append(params, info.Version.StructFieldName)
	}
	query = query + condition[5:]
	info.UpdateQuery.Query = query
	info.UpdateQuery.Params = params
}

func (info *tableInfo) genDelete() {
	query := ""
	argIdx := 0
	args := ""
	params := make([]string, 0, 1)
	if info.DeleteBY != nil || info.DeleteAT != nil {
		query = "UPDATE "
		if info.Namespace != "" {
			query = query + info.Namespace + "." + info.Name
		} else {
			query = query + info.Name
		}
		query = query + " SET "
		if info.DeleteBY != nil {
			argIdx++
			if info.Driver == "postgres" {
				args = args + ", " + fmt.Sprintf("%s=$%d", info.DeleteBY.Name, argIdx)
			} else {
				args = args + ", " + fmt.Sprintf("%s=?", info.DeleteBY.Name)
			}
			params = append(params, info.DeleteBY.StructFieldName)
		}
		if info.DeleteAT != nil {
			argIdx++
			if info.Driver == "postgres" {
				args = args + ", " + fmt.Sprintf("%s=$%d", info.DeleteAT.Name, argIdx)
			} else {
				args = args + ", " + fmt.Sprintf("%s=?", info.DeleteAT.Name)
			}
			params = append(params, info.DeleteAT.StructFieldName)
		}
		if info.Version != nil {
			args = args + ", " + fmt.Sprintf("%s=%s+1", info.Version.Name, info.Version.Name)
		}
		query = query + args[2:] + " WHERE "
		condition := ""
		for _, column := range info.Pks {
			argIdx++
			if info.Driver == "postgres" {
				condition = condition + " AND " + fmt.Sprintf("%s=$%d", column.Name, argIdx)
			} else {
				condition = condition + " AND " + fmt.Sprintf("%s=?", column.Name)
			}
			params = append(params, column.StructFieldName)
		}
		if info.Version != nil {
			argIdx++
			if info.Driver == "postgres" {
				condition = condition + " AND " + fmt.Sprintf("%s=$%d", info.Version.Name, argIdx)
			} else {
				condition = condition + " AND " + fmt.Sprintf("%s=?", info.Version.Name)
			}
			params = append(params, info.Version.StructFieldName)
		}
		query = query + condition[5:]
	} else {
		query = "DELETE FROM "
		if info.Namespace != "" {
			query = query + info.Namespace + "." + info.Name
		} else {
			query = query + info.Name
		}
		query = query + " WHERE "
		condition := ""
		for _, column := range info.Pks {
			argIdx++
			if info.Driver == "postgres" {
				condition = condition + " AND " + fmt.Sprintf("%s=$%d", column.Name, argIdx)
			} else {
				condition = condition + " AND " + fmt.Sprintf("%s=?", column.Name)
			}
			params = append(params, column.StructFieldName)
		}
		query = query + condition[5:]
	}

	info.DeleteQuery.Query = query
	info.DeleteQuery.Params = params
}

type queryInfo struct {
	Query  string
	Params []string
}

type columnInfo struct {
	Name            string
	Type            reflect.Type
	StructFieldName string
	IsJson          bool
}

type virtualColumnInfo struct {
	Name            string
	Type            reflect.Type
	StructFieldName string
	Source          string
}

type foreignColumnInfo struct {
	Name            string
	Type            reflect.Type
	StructFieldName string
	Sync            bool
}

type linkColumnInfo struct {
	Sync            bool
	LeftColumn      string
	RightColumn     string
	OrderBy         string
	SliceType       reflect.Type
	ElementType     reflect.Type
	StructFieldName string
}

// +-------------------------------------------------------------------------------------------------------------------+

func mapRelationName(name string) string {
	if driver == "postgres" {
		if strings.Index(name, "\"") == 0 {
			return name
		}
		return `"` + name + `"`
	}
	return name
}
