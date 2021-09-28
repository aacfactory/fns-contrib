package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"reflect"
)

func (d *dao) Query(ctx fns.Context, param *QueryParam, rows interface{}) (has bool, err errors.CodeError) {
	if rows == nil {
		panic(fmt.Sprintf("fns SQL: use DAO failed for row can not be nil"))
	}
	info := getTableRowInfo(rows)
	query := ""
	params := NewTuple()
	if param == nil {
		query = info.SimpleQuery
	} else {
		ns := info.Namespace
		name := info.Name
		alias := info.Alias
		if dialect == "postgres" {
			ns = tableInfoConvertToPostgresName(ns)
			name = tableInfoConvertToPostgresName(name)
			alias = tableInfoConvertToPostgresName(alias)
		}
		innerAlias := "__i"
		if dialect == "postgres" {
			innerAlias = tableInfoConvertToPostgresName(innerAlias)
		}
		innerQuery := "SELECT"
		pks := make([]string, 0, 1)
		for i, pk := range info.Pks {
			col := pk.Name
			if dialect == "postgres" {
				col = tableInfoConvertToPostgresName(col)
			}
			pks = append(pks, col)
			if i == 0 {
				innerQuery = innerQuery + " " + innerAlias + "." + col
			} else {
				innerQuery = innerQuery + ", " + innerAlias + "." + col
			}
		}
		if ns != "" {
			innerQuery = innerQuery + " FROM " + ns + "." + name + " AS " + innerAlias
		} else {
			innerQuery = innerQuery + " FROM " + name + " AS " + innerAlias
		}
		innerQuery = innerQuery + " " + param.mapToConditionString(innerAlias, params)
		innerQuery = innerQuery + " " + param.mapToSortsString(innerAlias)
		innerQuery = innerQuery + " " + param.mapToRangeString()
		query = "SELECT " + info.Selects
		if ns != "" {
			query = query + " FROM " + ns + "." + name + " AS " + alias
		} else {
			query = query + " FROM " + name + " AS " + alias
		}
		query = query + " INNER JOIN (" + innerQuery + ") AS " + innerAlias + " ON "
		for i, pk := range pks {
			if i == 0 {
				query = query + alias + "." + pk + " = " + innerAlias + "." + pk
			} else {
				query = query + " AND " + alias + "." + pk + " = " + innerAlias + "." + pk
			}
		}
		query = query + " " + param.mapToSortsString(alias)
	}
	// do
	results, queryErr := Query(ctx, Param{
		Query: query,
		Args:  params,
	})
	if queryErr != nil {
		err = queryErr
		return
	}
	if results.Empty() {
		return
	}
	scanErr := results.Scan(rows)
	if scanErr != nil {
		err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Query").WithCause(scanErr)
		return
	}
	rt := reflect.TypeOf(rows).Elem()
	if rt.Kind() == reflect.Slice {
		rv := reflect.ValueOf(rows).Elem()
		size := rv.Len()
		for i := 0; i < size; i++ {
			rev := rv.Index(i).Interface()
			tr, ok := rev.(TableRow)
			if !ok {
				panic(fmt.Sprintf("fns SQL: use DAO failed for row is not sql.TableRow"))
			}
			fillErr := d.fillRow(ctx, tr)
			if fillErr != nil {
				err = fillErr
				return
			}
		}
	} else {
		rv := reflect.ValueOf(rows).Interface()
		tr, ok := rv.(TableRow)
		if !ok {
			panic(fmt.Sprintf("fns SQL: use DAO failed for row is not sql.TableRow"))
		}
		fillErr := d.fillRow(ctx, tr)
		if fillErr != nil {
			err = fillErr
			return
		}
	}
	has = true
	return
}

func (d *dao) fillRow(ctx fns.Context, row TableRow) (err errors.CodeError) {
	hasGot, synced := d.Cache.GetAndFill(row)
	if synced {
		return
	}
	if !hasGot {
		d.Cache.Set(row, false)
	}
	info := getTableRowInfo(row)
	rv := reflect.Indirect(reflect.ValueOf(row))
	// fk
	for _, column := range info.ForeignColumns {
		frv := rv.FieldByName(column.StructFieldName)
		if frv.IsNil() {
			continue
		}
		x := frv.Interface()
		xRow, mapOk := x.(TableRow)
		if !mapOk {
			panic(fmt.Sprintf("fns SQL: use DAO failed for %s of row is not sql.TableRow", column.StructFieldName))
		}
		_, loadFCErr := d.Get(ctx, xRow)
		if loadFCErr != nil {
			err = loadFCErr
			return
		}
		frv.Set(reflect.ValueOf(x))
	}
	// lk
	for _, column := range info.LinkColumns {
		lrv := rv.FieldByName(column.StructFieldName)
		lkHasRef := false
		if lrv.Len() == 0 {
			lkInfo := getTableRowInfo(reflect.New(column.ElementType.Elem()).Interface())
			lkHasRef = len(lkInfo.ForeignColumns) != 0 || len(lkInfo.LinkColumns) != 0
			linkQuery := lkInfo.genLinkQuery(column)
			leftField := info.GetColumnField(column.LeftColumn)
			left := rv.FieldByName(leftField).Interface()
			lkParams := NewTuple().Append(left)
			linkRows, queryLinkErr := Query(ctx, Param{
				Query: linkQuery,
				Args:  lkParams,
			})
			if queryLinkErr != nil {
				err = queryLinkErr
				return
			}
			if linkRows.Empty() {
				continue
			}
			x := reflect.MakeSlice(column.SliceType, 0, 1).Interface()
			scanLinkErr := linkRows.Scan(&x)
			if scanLinkErr != nil {
				err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Get").WithCause(scanLinkErr)
				return
			}
			lrv.Set(reflect.ValueOf(x))
		}
		if lkHasRef {
			for i := 0; i < lrv.Len(); i++ {
				rxe := lrv.Index(i).Interface()
				xRow, mapOk := rxe.(TableRow)
				if !mapOk {
					panic(fmt.Sprintf("fns SQL: use DAO failed for %s of row is not sql.TableRow", column.StructFieldName))
				}
				_, loadLCErr := d.Get(ctx, xRow)
				if loadLCErr != nil {
					err = loadLCErr
					return
				}
			}
		}
	}
	d.Cache.Set(row, true)
	return
}
