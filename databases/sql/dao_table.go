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

func getTableRowInfo(target interface{}) (info *tableInfo) {
	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("fns SQL: use DAO failed for target must be ptr"))
	}
	rt = rt.Elem()
	if rt.Kind() == reflect.Struct {
		key := fmt.Sprintf("%s:%s", rt.PkgPath(), rt.Name())
		cached, hasCache := tableInfoMap.Load(key)
		if hasCache {
			info = cached.(*tableInfo)
			return
		}
		table, convertOk := target.(TableRow)
		if !convertOk {
			panic(fmt.Sprintf("fns SQL: use DAO failed for %s/%s is not TableRow implement", rt.PkgPath(), rt.Name()))
		}
		info = newTableInfo(table)
		tableInfoMap.Store(key, info)
	} else if rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
		rt = rt.Elem()
		if rt.Kind() != reflect.Ptr {
			panic(fmt.Sprintf("fns SQL: use DAO failed for element of slice target must be ptr struct"))
		}
		if rt.Elem().Kind() != reflect.Struct {
			panic(fmt.Sprintf("fns SQL: use DAO failed for element of slice target must be ptr struct"))
		}
		xrt := rt.Elem()
		key := fmt.Sprintf("%s:%s", xrt.PkgPath(), xrt.Name())
		cached, hasCache := tableInfoMap.Load(key)
		if hasCache {
			info = cached.(*tableInfo)
			return
		}
		x := reflect.New(rt.Elem()).Interface()
		table, convertOk := x.(TableRow)
		if !convertOk {
			panic(fmt.Sprintf("fns SQL: use DAO failed for %s/%s is not TableRow implement", rt.PkgPath(), rt.Name()))
		}
		info = newTableInfo(table)
		tableInfoMap.Store(key, info)
	} else {
		panic(fmt.Sprintf("fns SQL: use DAO failed for element of target must be struct of slice"))
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

func newTableInfo(table TableRow) (info *tableInfo) {
	rt := reflect.TypeOf(table).Elem()
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
	info = &tableInfo{
		Namespace:             namespace,
		Name:                  name,
		Alias:                 alias,
		Pks:                   make([]*columnInfo, 0, 1),
		CreateBY:              nil,
		CreateAT:              nil,
		ModifyBY:              nil,
		ModifyAT:              nil,
		DeleteBY:              nil,
		DeleteAT:              nil,
		Version:               nil,
		Columns:               make([]*columnInfo, 0, 1),
		ForeignColumns:        make([]*foreignColumnInfo, 0, 1),
		LinkColumns:           make([]*linkColumnInfo, 0, 1),
		VirtualColumns:        make([]*virtualColumnInfo, 0, 1),
		InsertQuery:           queryInfo{},
		UpdateQuery:           queryInfo{},
		DeleteQuery:           queryInfo{},
		SaveQuery:             queryInfo{},
		GetQuery:              queryInfo{},
		ExistQuery:            queryInfo{},
		LinkQueryMap:          make(map[string]queryInfo),
		LinkSaveCleanQueryMap: make(map[string]queryInfo),
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
			column := &columnInfo{
				Name:            columnName,
				Type:            field.Type,
				StructFieldName: field.Name,
			}
			info.Columns = append(info.Columns, column)
			continue
		}
		columnName := tag[0:strings.Index(tag, ",")]
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
						panic(fmt.Sprintf("fns SQL: use DAO failed for LK Tag must use ref to define link columns, %s/%s", rt.PkgPath(), rt.Name()))
					}
					refLeftColumn = strings.TrimSpace(refs[0])
					refRightColumn = strings.TrimSpace(refs[1])
				}
				if refLeftColumn == "" || refRightColumn == "" {
					panic(fmt.Sprintf("fns SQL: use DAO failed for LK Tag must use ref to define link columns, %s/%s", rt.PkgPath(), rt.Name()))
				}
				sorts, _ := field.Tag.Lookup(sortTag)
				sorts = strings.ToUpper(strings.TrimSpace(sorts))
				lkOrderBy := make([]linkColumnOrderBy, 0, 1)
				if sorts != "" {
					sortItems := strings.Split(sorts, ",")
					for _, item := range sortItems {
						item = strings.TrimSpace(item)
						if strings.Contains(item, " ") {
							colIdx := strings.Index(item, " ")
							orderByCol := item[0:colIdx]
							orderByKind := item[colIdx:]
							lkOrderBy = append(lkOrderBy, linkColumnOrderBy{
								Column: orderByCol,
								Asc:    orderByKind == "ASC",
							})
						}
					}
				}

				info.LinkColumns = append(info.LinkColumns, &linkColumnInfo{
					Sync:            defineOp == "SYNC",
					LeftColumn:      refLeftColumn,
					RightColumn:     refRightColumn,
					OrderBy:         lkOrderBy,
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
	switch dialect {
	case "postgres":
		tableInfoGenPostgresInsertQuery(info)
		tableInfoGenPostgresUpdateQuery(info)
		tableInfoGenPostgresDeleteQuery(info)
		tableInfoGenPostgresSaveQuery(info)
		tableInfoGenPostgresGetQuery(info)
		tableInfoGenPostgresExistQuery(info)
		tableInfoGenPostgresVirtualQuery(info)
		tableInfoGenPostgresLinkQuery(info)
		tableInfoGenPostgresLinkSaveCleanQuery(info)
	case "mysql":
		tableInfoGenMysqlInsertQuery(info)
		tableInfoGenMysqlUpdateQuery(info)
		tableInfoGenMysqlDeleteQuery(info)
		tableInfoGenMysqlSaveQuery(info)
		tableInfoGenMysqlGetQuery(info)
		tableInfoGenMysqlExistQuery(info)
		tableInfoGenMysqlLinkQuery(info)
		tableInfoGenMysqlLinkSaveCleanQuery(info)
	default:
		panic(fmt.Sprintf("fns SQL: use DAO but dialect(%s) was not supported", dialect))
	}
	return
}

type tableInfo struct {
	Namespace             string
	Name                  string
	Alias                 string
	Selects               string
	Pks                   []*columnInfo
	CreateBY              *columnInfo
	CreateAT              *columnInfo
	ModifyBY              *columnInfo
	ModifyAT              *columnInfo
	DeleteBY              *columnInfo
	DeleteAT              *columnInfo
	Version               *columnInfo
	Columns               []*columnInfo
	ForeignColumns        []*foreignColumnInfo
	LinkColumns           []*linkColumnInfo
	VirtualColumns        []*virtualColumnInfo
	SimpleQuery           string
	InsertQuery           queryInfo
	UpdateQuery           queryInfo
	DeleteQuery           queryInfo
	SaveQuery             queryInfo
	GetQuery              queryInfo
	ExistQuery            queryInfo
	VirtualQuery 		  *queryInfo
	LinkQueryMap          map[string]queryInfo // key=fk_name
	LinkSaveCleanQueryMap map[string]queryInfo // key=fk_name
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

func (info *tableInfo) genLinkQuery(link *linkColumnInfo) (query string) {
	qi, has := info.LinkQueryMap[link.RightColumn]
	if !has {
		panic(fmt.Sprintf("fns SQL: use DAO but get link query failed, rigtht was not defined int table row"))
	}
	alias := info.Alias
	if dialect == "postgres" {
		alias = tableInfoConvertToPostgresName(alias)
	}
	query = qi.Query
	if link.OrderBy != nil && len(link.OrderBy) > 0 {
		orderBy := ""
		for i, s := range link.OrderBy {
			col := s.Column
			if dialect == "postgres" {
				col = tableInfoConvertToPostgresName(col)
			}
			kind := "DESC"
			if s.Asc {
				kind = "ASC"
			}
			if i == 0 {
				orderBy = alias + "." + col + " " + kind
			} else {
				orderBy = alias + "." + orderBy + ", " + col + " " + kind
			}
		}
		query = query + " ORDER BY " + orderBy
	}
	return
}

func (info *tableInfo) genLinkSaveCleanQuery(link *linkColumnInfo, actives int) (query string) {
	qi, has := info.LinkSaveCleanQueryMap[link.RightColumn]
	if !has {
		panic(fmt.Sprintf("fns SQL: use DAO but get link query failed, rigtht was not defined int table row"))
	}
	alias := info.Alias
	if dialect == "postgres" {
		alias = tableInfoConvertToPostgresName(alias)
	}
	query = qi.Query

	if actives > 0 {
		col := link.LeftColumn
		if dialect == "postgres" {
			col = tableInfoConvertToPostgresName(col)
		}
		query = query + " AND " + alias + "." + col + " NOT IN ("
		for i := 1; i <= actives; i++ {
			if i == 1 {
				if dialect == "postgres" {
					query = query + fmt.Sprintf("$%d", i+1)
				} else {
					query = query + "?"
				}
			} else {
				if dialect == "postgres" {
					query = query + "," + fmt.Sprintf("$%d", i+1)
				} else {
					query = query + ",?"
				}
			}
		}
		query = query + ")"
	}
	return
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
	OrderBy         []linkColumnOrderBy
	SliceType       reflect.Type
	ElementType     reflect.Type
	StructFieldName string
}

type linkColumnOrderBy struct {
	Column string
	Asc    bool
}
