package sql

import (
	"fmt"
)

func tableInfoGenMysqlInsertQuery(info *tableInfo) {
	ns := info.Namespace
	name := info.Name
	query := "INSERT IGNORE INTO "
	if ns != "" {
		query = query + ns + "." + name
	} else {
		query = query + name
	}
	params := make([]string, 0, 1)
	args := ""
	query = query + " ("
	pks := ""
	for i, pk := range info.Pks {
		if i == 0 {
			pks = pks + pk.Name
		} else {
			pks = pks + ", " + pk.Name
		}
		args = args + ", ?"
		params = append(params, pk.StructFieldName)
	}
	query = query + pks
	if info.CreateBY != nil {
		query = query + ", " + info.CreateBY.Name
		args = args + ", ?"
		params = append(params, info.CreateBY.StructFieldName)
	}
	if info.CreateAT != nil {
		query = query + ", " + info.CreateAT.Name
		args = args + ", ?"
		params = append(params, info.CreateAT.StructFieldName)
	}
	if info.Version != nil {
		query = query + ", " + info.Version.Name
		args = args + ", ?"
		params = append(params, info.Version.StructFieldName)
	}
	for _, column := range info.Columns {
		query = query + ", " + column.Name
		args = args + ", ?"
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		query = query + ", " + column.Name
		args = args + ", ?"
		params = append(params, column.StructFieldName)
	}
	query = query + ") VALUES (" + args[2:] + ")"
	info.InsertQuery.Query = query
	info.InsertQuery.Params = params
}

func tableInfoGenMysqlUpdateQuery(info *tableInfo) {
	ns := info.Namespace
	name := info.Name
	query := "UPDATE "
	if ns != "" {
		query = query + ns + "." + name
	} else {
		query = query + name
	}
	query = query + " SET "
	args := ""
	params := make([]string, 0, 1)
	if info.ModifyBY != nil {
		args = args + ", " + fmt.Sprintf("%s=?", info.ModifyBY.Name)
		params = append(params, info.ModifyBY.StructFieldName)
	}
	if info.ModifyAT != nil {
		args = args + ", " + fmt.Sprintf("%s=?", info.ModifyAT.Name)
		params = append(params, info.ModifyAT.StructFieldName)
	}
	if info.Version != nil {
		args = args + ", " + fmt.Sprintf("%s=%s+1", info.Version.Name, info.Version.Name)
	}
	for _, column := range info.Columns {
		args = args + ", " + fmt.Sprintf("%s=?", column.Name)
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		args = args + ", " + fmt.Sprintf("%s=?", column.Name)
		params = append(params, column.StructFieldName)
	}
	query = query + args[2:] + " WHERE "
	condition := ""
	for _, column := range info.Pks {
		condition = condition + " AND " + fmt.Sprintf("%s=?", column.Name)
		params = append(params, column.StructFieldName)
	}
	if info.Version != nil {
		condition = condition + " AND " + fmt.Sprintf("%s=?", info.Version.Name)
		params = append(params, info.Version.StructFieldName)
	}
	query = query + condition[5:]
	info.UpdateQuery.Query = query
	info.UpdateQuery.Params = params
}

func tableInfoGenMysqlDeleteQuery(info *tableInfo) {
	ns := info.Namespace
	name := info.Name
	query := ""
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
			args = args + ", " + fmt.Sprintf("%s=?", info.DeleteBY.Name)
			params = append(params, info.DeleteBY.StructFieldName)
		}
		if info.DeleteAT != nil {
			args = args + ", " + fmt.Sprintf("%s=?", info.DeleteAT.Name)
			params = append(params, info.DeleteAT.StructFieldName)
		}
		if info.Version != nil {
			args = args + ", " + fmt.Sprintf("%s=%s+1", info.Version.Name, info.Version.Name)
		}
		query = query + args[2:] + " WHERE "
		condition := ""
		for _, column := range info.Pks {
			condition = condition + " AND " + fmt.Sprintf("%s=?", column.Name)
			params = append(params, column.StructFieldName)
		}
		if info.Version != nil {
			condition = condition + " AND " + fmt.Sprintf("%s=?", info.Version.Name)
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
			condition = condition + " AND " + fmt.Sprintf("%s=?", column.Name)
			params = append(params, column.StructFieldName)
		}
		query = query + condition[5:]
	}

	info.DeleteQuery.Query = query
	info.DeleteQuery.Params = params
}

func tableInfoGenMysqlSaveQuery(info *tableInfo) {
	ns := info.Namespace
	name := info.Name
	query := "INSERT INTO "
	if ns != "" {
		query = query + ns + "." + name
	} else {
		query = query + name
	}
	params := make([]string, 0, 1)
	args := ""
	query = query + " ("
	pks := ""
	for i, pk := range info.Pks {
		if i == 0 {
			pks = pks + pk.Name
		} else {
			pks = pks + ", " + pk.Name
		}
		args = args + ", ?"
		params = append(params, pk.StructFieldName)
	}
	query = query + pks
	if info.CreateBY != nil {
		query = query + ", " + info.CreateBY.Name
		args = args + ", ?"
		params = append(params, info.CreateBY.StructFieldName)
	}
	if info.CreateAT != nil {
		query = query + ", " + info.CreateAT.Name
		args = args + ", ?"
		params = append(params, info.CreateAT.StructFieldName)
	}
	if info.Version != nil {
		query = query + ", " + info.Version.Name
		args = args + ", ?"
		params = append(params, info.Version.StructFieldName)
	}
	for _, column := range info.Columns {
		query = query + ", " + column.Name
		args = args + ", ?"
		params = append(params, column.StructFieldName)
	}
	for _, column := range info.ForeignColumns {
		query = query + ", " + column.Name
		args = args + ", ?"
		params = append(params, column.StructFieldName)
	}
	query = query + ") VALUES (" + args[2:] + ")"
	query = query + " ON DUPLICATE KEY UPDATE"
	updateArgs := ""
	if info.ModifyBY != nil {
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=VALUES(%s)", info.ModifyBY.Name, info.ModifyBY.Name)
	}
	if info.ModifyAT != nil {
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=VALUES(%s)", info.ModifyAT.Name, info.ModifyAT.Name)
	}
	if info.Version != nil {
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=%s+1", info.Version.Name, info.Version.Name)
	}
	for _, column := range info.Columns {
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=VALUES(%s)", column.Name, column.Name)
	}
	for _, column := range info.ForeignColumns {
		updateArgs = updateArgs + ", " + fmt.Sprintf("%s=VALUES(%s)", column.Name, column.Name)
	}
	query = query + " " + updateArgs[2:]
	info.SaveQuery.Query = query
	info.SaveQuery.Params = params
}

func tableInfoGenMysqlGetQuery(info *tableInfo) {
	ns := info.Namespace
	name := info.Name
	alias := info.Alias
	query := "SELECT"
	selects := ""
	// pk
	for _, pk := range info.Pks {
		selects = selects + ", " + alias + "." + pk.Name
	}
	// audit
	if info.CreateBY != nil {
		selects = selects + ", " + alias + "." + info.CreateBY.Name
	}
	if info.CreateAT != nil {
		selects = selects + ", " + alias + "." + info.CreateAT.Name
	}
	if info.ModifyBY != nil {
		selects = selects + ", " + alias + "." + info.ModifyBY.Name
	}
	if info.ModifyAT != nil {
		selects = selects + ", " + alias + "." + info.ModifyAT.Name
	}
	if info.DeleteBY != nil {
		selects = selects + ", " + alias + "." + info.DeleteBY.Name
	}
	if info.DeleteAT != nil {
		selects = selects + ", " + alias + "." + info.DeleteAT.Name
	}
	if info.Version != nil {
		selects = selects + ", " + alias + "." + info.Version.Name
	}
	// col
	for _, column := range info.Columns {
		selects = selects + ", " + alias + "." + column.Name
	}
	// fk
	for _, column := range info.ForeignColumns {
		selects = selects + ", " + alias + "." + column.Name
	}
	// vc
	for _, column := range info.VirtualColumns {
		selects = selects + ", (" + column.Source + ") AS " + column.Name
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
			query = query + alias + "." + pk.Name + "=?"

		} else {
			query = query + "AND " + alias + "." + pk.Name + "=?"
		}
		params = append(params, pk.StructFieldName)
	}
	info.GetQuery.Query = query
	info.GetQuery.Params = params
}

func tableInfoGenMysqlExistQuery(info *tableInfo) {
	ns := info.Namespace
	name := info.Name
	alias := info.Alias
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
			query = query + alias + "." + pk.Name + "=?"
		} else {
			query = query + "AND " + alias + "." + pk.Name + "=?"
		}
		params = append(params, pk.StructFieldName)
	}
	info.ExistQuery.Query = query
	info.ExistQuery.Params = params
}

func tableInfoGenMysqlLinkQuery(info *tableInfo) {
	if len(info.ForeignColumns) == 0 {
		return
	}
	ns := info.Namespace
	name := info.Name
	alias := info.Alias
	for _, fc := range info.ForeignColumns {
		query := "SELECT "
		selects := ""
		// pk
		for _, pk := range info.Pks {
			selects = selects + ", " + alias + "." + pk.Name
		}
		// audit
		if info.CreateBY != nil {
			selects = selects + ", " + alias + "." + info.CreateBY.Name
		}
		if info.CreateAT != nil {
			selects = selects + ", " + alias + "." + info.CreateAT.Name
		}
		if info.ModifyBY != nil {
			selects = selects + ", " + alias + "." + info.ModifyBY.Name
		}
		if info.ModifyAT != nil {
			selects = selects + ", " + alias + "." + info.ModifyAT.Name
		}
		if info.DeleteBY != nil {
			selects = selects + ", " + alias + "." + info.DeleteBY.Name
		}
		if info.DeleteAT != nil {
			selects = selects + ", " + alias + "." + info.DeleteAT.Name
		}
		if info.Version != nil {
			selects = selects + ", " + alias + "." + info.Version.Name
		}
		// col
		for _, column := range info.Columns {
			selects = selects + ", " + alias + "." + column.Name
		}
		// fk
		for _, column := range info.ForeignColumns {
			selects = selects + ", " + alias + "." + column.Name
		}
		// vc
		for _, column := range info.VirtualColumns {
			selects = selects + ", (" + column.Source + ") AS " + column.Name
		}

		query = query + selects[1:]
		if ns != "" {
			query = query + " FROM " + ns + "." + name + " AS " + alias
		} else {
			query = query + " FROM " + name + " AS " + alias
		}
		query = query + " WHERE "

		query = query + alias + "." + fc.Name + "=?"

		info.LinkQueryMap[fc.Name] = queryInfo{
			Query:  query,
			Params: nil,
		}
	}

}

func tableInfoGenMysqlLinkSaveCleanQuery(info *tableInfo) {
	if len(info.ForeignColumns) == 0 {
		return
	}
	ns := info.Namespace
	name := info.Name
	alias := info.Alias
	for _, fc := range info.ForeignColumns {
		query := "DELETE "
		if ns != "" {
			query = query + " FROM " + ns + "." + name + " AS " + alias
		} else {
			query = query + " FROM " + name + " AS " + alias
		}
		query = query + " WHERE "
		query = query + tableInfoConvertToPostgresName(fc.Name) + " =? "
		info.LinkSaveCleanQueryMap[fc.Name] = queryInfo{
			Query:  query,
			Params: nil,
		}
	}
}
