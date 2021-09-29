package sql

import (
	"fmt"
	"strings"
)

func tableInfoConvertToPostgresName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if strings.Index(name, "\"") == 0 {
		return name
	}
	return `"` + name + `"`
}

func tableInfoGenPostgresInsertQuery(info *tableInfo) {
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	query := "INSERT INTO "
	if ns != "" {
		query = query + ns + "." + name
	} else {
		query = query + name
	}
	params := make([]string, 0, 1)
	argIdx := 0
	args := ""
	query = query + " ("
	pks := ""
	for i, pk := range info.Pks {
		if i == 0 {
			pks = pks + tableInfoConvertToPostgresName(pk.Name)
		} else {
			pks = pks + ", " + tableInfoConvertToPostgresName(pk.Name)
		}
		argIdx++
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, pk.StructFieldName)
	}
	query = query + pks
	if info.CreateBY != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.CreateBY.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.CreateBY.StructFieldName)
	}
	if info.CreateAT != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.CreateAT.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.CreateAT.StructFieldName)
	}
	if info.Version != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.Version.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.Version.StructFieldName)
	}
	for _, column := range info.Columns {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(column.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(column.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, column.StructFieldName)
	}
	query = query + ") VALUES (" + args[2:] + ")" + " ON CONFLICT (" + pks + ") DO NOTHING"
	info.InsertQuery.Query = query
	info.InsertQuery.Params = params
}

func tableInfoGenPostgresUpdateQuery(info *tableInfo) {
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	query := "UPDATE "
	if ns != "" {
		query = query + ns + "." + name
	} else {
		query = query + name
	}
	query = query + " SET "
	argIdx := 0
	args := ""
	params := make([]string, 0, 1)
	if info.ModifyBY != nil {
		argIdx++
		args = args + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.ModifyBY.Name), argIdx)
		params = append(params, info.ModifyBY.StructFieldName)
	}
	if info.ModifyAT != nil {
		argIdx++
		args = args + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.ModifyAT.Name), argIdx)
		params = append(params, info.ModifyAT.StructFieldName)
	}
	if info.Version != nil {
		args = args + ", " + fmt.Sprintf("%s=%s+1", tableInfoConvertToPostgresName(info.Version.Name), tableInfoConvertToPostgresName(info.Version.Name))
	}
	for _, column := range info.Columns {
		argIdx++
		args = args + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), argIdx)
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		argIdx++
		args = args + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), argIdx)
		params = append(params, column.StructFieldName)
	}
	query = query + args[2:] + " WHERE "
	condition := ""
	for _, column := range info.Pks {
		argIdx++
		condition = condition + " AND " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), argIdx)
		params = append(params, column.StructFieldName)
	}
	if info.Version != nil {
		argIdx++
		condition = condition + " AND " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.Version.Name), argIdx)
		params = append(params, info.Version.StructFieldName)
	}
	query = query + condition[5:]
	info.UpdateQuery.Query = query
	info.UpdateQuery.Params = params
}

func tableInfoGenPostgresDeleteQuery(info *tableInfo) {
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	query := ""
	argIdx := 0
	args := ""
	params := make([]string, 0, 1)
	if info.DeleteBY != nil || info.DeleteAT != nil {
		query = "UPDATE "
		if ns != "" {
			query = query + ns + "." + name
		} else {
			query = query + name
		}
		query = query + " SET "
		if info.DeleteBY != nil {
			argIdx++
			args = args + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.DeleteBY.Name), argIdx)
			params = append(params, info.DeleteBY.StructFieldName)
		}
		if info.DeleteAT != nil {
			argIdx++
			args = args + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.DeleteAT.Name), argIdx)
			params = append(params, info.DeleteAT.StructFieldName)
		}
		if info.Version != nil {
			args = args + ", " + fmt.Sprintf("%s=%s+1", tableInfoConvertToPostgresName(info.Version.Name), tableInfoConvertToPostgresName(info.Version.Name))
		}
		query = query + args[2:] + " WHERE "
		condition := ""
		for _, column := range info.Pks {
			argIdx++
			condition = condition + " AND " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), argIdx)
			params = append(params, column.StructFieldName)
		}
		if info.Version != nil {
			argIdx++
			condition = condition + " AND " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.Version.Name), argIdx)
			params = append(params, info.Version.StructFieldName)
		}
		query = query + condition[5:]
	} else {
		query = "DELETE FROM "
		if ns != "" {
			query = query + ns + "." + name
		} else {
			query = query + name
		}
		query = query + " WHERE "
		condition := ""
		for _, column := range info.Pks {
			argIdx++
			condition = condition + " AND " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), argIdx)
			params = append(params, column.StructFieldName)
		}
		query = query + condition[5:]
	}

	info.DeleteQuery.Query = query
	info.DeleteQuery.Params = params
}

func tableInfoGenPostgresSaveQuery(info *tableInfo) {
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	query := "INSERT INTO "
	if ns != "" {
		query = query + ns + "." + name
	} else {
		query = query + name
	}
	params := make([]string, 0, 1)
	argIdx := 0
	args := ""
	query = query + " ("
	pks := ""
	for i, pk := range info.Pks {
		if i == 0 {
			pks = pks + tableInfoConvertToPostgresName(pk.Name)
		} else {
			pks = pks + ", " + tableInfoConvertToPostgresName(pk.Name)
		}
		argIdx++
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, pk.StructFieldName)
	}
	query = query + pks
	if info.CreateBY != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.CreateBY.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.CreateBY.StructFieldName)
	}
	if info.CreateAT != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.CreateAT.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.CreateAT.StructFieldName)
	}
	if info.ModifyBY != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.ModifyBY.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.ModifyBY.StructFieldName)
	}
	if info.ModifyAT != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.ModifyAT.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.ModifyAT.StructFieldName)
	}
	if info.Version != nil {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(info.Version.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, info.Version.StructFieldName)
	}
	for _, column := range info.Columns {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(column.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		argIdx++
		query = query + ", " + tableInfoConvertToPostgresName(column.Name)
		args = args + ", " + fmt.Sprintf("$%d", argIdx)
		params = append(params, column.StructFieldName)
	}
	query = query + ") VALUES (" + args[2:] + ")" + " ON CONFLICT (" + pks + ") DO "

	updateArgs := ""
	updateArgIdx := 1
	query = query + "UPDATE SET "
	if info.CreateBY != nil {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.CreateBY.Name), updateArgIdx)
	}
	if info.CreateAT != nil {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.CreateAT.Name), updateArgIdx)
	}
	if info.ModifyBY != nil {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.ModifyBY.Name), updateArgIdx)
	}
	if info.ModifyAT != nil {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.ModifyAT.Name), updateArgIdx)
	}
	if info.Version != nil {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(info.Version.Name), updateArgIdx)
	}
	for _, column := range info.Columns {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), updateArgIdx)
	}
	for _, column := range info.ForeignColumns {
		updateArgIdx++
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=$%d", tableInfoConvertToPostgresName(column.Name), updateArgIdx)
	}
	query = query + updateArgs[2:]
	info.SaveQuery.Query = query
	info.SaveQuery.Params = params
}

func tableInfoGenPostgresVirtualQuery(info *tableInfo) {
	if info.VirtualColumns == nil || len(info.VirtualColumns) == 0 {
		return
	}
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	alias := tableInfoConvertToPostgresName(info.Alias)
	query := "SELECT"
	selects := ""
	// vc
	for _, column := range info.VirtualColumns {
		selects = selects + ", (" + column.Source + ") AS " + tableInfoConvertToPostgresName(column.Name)
	}
	query = query + selects[1:]
	if ns != "" {
		query = query + " FROM " + ns + "." + name + " AS " + alias
	} else {
		query = query + " FROM " + name + " AS " + alias
	}
	info.SimpleQuery = query
	params := make([]string, 0, 1)
	query = query + " WHERE "
	for i, pk := range info.Pks {
		if i == 0 {
			query = query + alias + "." + tableInfoConvertToPostgresName(pk.Name) + fmt.Sprintf("=$%d", i+1)

		} else {
			query = query + "AND " + alias + "." + tableInfoConvertToPostgresName(pk.Name) + fmt.Sprintf("=$%d", i+1)
		}
		params = append(params, pk.StructFieldName)
	}
	info.VirtualQuery = &queryInfo{
		Query:  query,
		Params: params,
	}
}

func tableInfoGenPostgresGetQuery(info *tableInfo) {
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	alias := tableInfoConvertToPostgresName(info.Alias)
	query := "SELECT"
	selects := ""
	// pk
	for _, pk := range info.Pks {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(pk.Name)
	}
	// audit
	if info.CreateBY != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.CreateBY.Name)
	}
	if info.CreateAT != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.CreateAT.Name)
	}
	if info.ModifyBY != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.ModifyBY.Name)
	}
	if info.ModifyAT != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.ModifyAT.Name)
	}
	if info.DeleteBY != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.DeleteBY.Name)
	}
	if info.DeleteAT != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.DeleteAT.Name)
	}
	if info.Version != nil {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.Version.Name)
	}
	// col
	for _, column := range info.Columns {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(column.Name)
	}
	// fk
	for _, column := range info.ForeignColumns {
		selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(column.Name)
	}

	info.Selects = selects[1:]
	query = query + info.Selects
	if ns != "" {
		query = query + " FROM " + ns + "." + name + " AS " + alias
	} else {
		query = query + " FROM " + name + " AS " + alias
	}
	info.SimpleQuery = query
	params := make([]string, 0, 1)
	query = query + " WHERE "
	for i, pk := range info.Pks {
		if i == 0 {
			query = query + alias + "." + tableInfoConvertToPostgresName(pk.Name) + fmt.Sprintf("=$%d", i+1)

		} else {
			query = query + "AND " + alias + "." + tableInfoConvertToPostgresName(pk.Name) + fmt.Sprintf("=$%d", i+1)
		}
		params = append(params, pk.StructFieldName)
	}
	info.GetQuery.Query = query
	info.GetQuery.Params = params
}

func tableInfoGenPostgresExistQuery(info *tableInfo) {
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	alias := tableInfoConvertToPostgresName(info.Alias)
	query := `SELECT 1 FROM `
	if ns != "" {
		query = query + ns + "." + name + " AS " + alias
	} else {
		query = query + name + " AS " + alias
	}
	params := make([]string, 0, 1)
	query = query + " WHERE "
	for i, pk := range info.Pks {
		if i == 0 {
			query = query + alias + "." + tableInfoConvertToPostgresName(pk.Name) + fmt.Sprintf("=$%d", i+1)
		} else {
			query = query + "AND " + alias + "." + tableInfoConvertToPostgresName(pk.Name) + fmt.Sprintf("=$%d", i+1)
		}
		params = append(params, pk.StructFieldName)
	}
	info.ExistQuery.Query = query
	info.ExistQuery.Params = params
}

func tableInfoGenPostgresLinkQuery(info *tableInfo) {
	if len(info.ForeignColumns) == 0 {
		return
	}
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	alias := tableInfoConvertToPostgresName(info.Alias)
	for _, fc := range info.ForeignColumns {
		query := "SELECT "
		selects := ""
		// pk
		for _, pk := range info.Pks {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(pk.Name)
		}
		// audit
		if info.CreateBY != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.CreateBY.Name)
		}
		if info.CreateAT != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.CreateAT.Name)
		}
		if info.ModifyBY != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.ModifyBY.Name)
		}
		if info.ModifyAT != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.ModifyAT.Name)
		}
		if info.DeleteBY != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.DeleteBY.Name)
		}
		if info.DeleteAT != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.DeleteAT.Name)
		}
		if info.Version != nil {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(info.Version.Name)
		}
		// col
		for _, column := range info.Columns {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(column.Name)
		}
		// fk
		for _, column := range info.ForeignColumns {
			selects = selects + ", " + alias + "." + tableInfoConvertToPostgresName(column.Name)
		}

		query = query + selects[1:]
		if ns != "" {
			query = query + " FROM " + ns + "." + name + " AS " + alias
		} else {
			query = query + " FROM " + name + " AS " + alias
		}
		query = query + " WHERE "

		query = query + alias + "." + tableInfoConvertToPostgresName(fc.Name) + "=$1"

		info.LinkQueryMap[fc.Name] = queryInfo{
			Query:  query,
			Params: nil,
		}
	}

}

func tableInfoGenPostgresLinkSaveCleanQuery(info *tableInfo) {
	if len(info.ForeignColumns) == 0 {
		return
	}
	ns := tableInfoConvertToPostgresName(info.Namespace)
	name := tableInfoConvertToPostgresName(info.Name)
	alias := tableInfoConvertToPostgresName(info.Alias)
	for _, fc := range info.ForeignColumns {
		query := "DELETE "
		if ns != "" {
			query = query + " FROM " + ns + "." + name + " AS " + alias
		} else {
			query = query + " FROM " + name + " AS " + alias
		}
		query = query + " WHERE "
		query = query + alias + "." + tableInfoConvertToPostgresName(fc.Name) + " =$1 "
		info.LinkSaveCleanQueryMap[fc.Name] = queryInfo{
			Query:  query,
			Params: nil,
		}
	}
}
