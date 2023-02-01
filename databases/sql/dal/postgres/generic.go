package postgres

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"reflect"
	"strconv"
	"strings"
)

func formatIdents(s ...string) (v string) {
	for _, n := range s {
		v = v + "." + `"` + strings.ToUpper(strings.TrimSpace(n)) + `"`
	}
	if v != "" {
		v = v[1:]
	}
	return
}

func newInsertGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	method := dal.Execute
	incrPk := ""
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	valuesIdx := 0
	conflictColumns := make([]string, 0, 1)
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsIncrPk() {
			method = dal.Query
			incrPk = field.Column()
			continue
		}
		if field.IsAMB() || field.IsAMT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() {
			continue
		}
		if field.IsAOL() {
			column := field.Column()
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesFragments = valuesFragments + `, 1`
			continue
		}
		columns := field.Columns()
		for _, column := range columns {
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesIdx++
			valuesFragments = valuesFragments + `, ` + fmt.Sprintf("$%d", valuesIdx)
		}
		if field.Conflict() {
			conflictColumns = append(conflictColumns, columns...)
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	columnsFragments = columnsFragments[2:]
	valuesFragments = valuesFragments[2:]
	ql := `INSERT INTO ` + tableName + ` ` + `(` + columnsFragments + `)` + ` VALUES (` + valuesFragments + `)`
	if len(conflictColumns) > 0 {
		conflicts := ""
		for _, conflictColumn := range conflictColumns {
			conflicts = conflicts + ", " + formatIdents(conflictColumn)
		}
		conflicts = conflicts[2:]
		ql = ql + ` ON CONFLICT (` + conflicts + `) DO NOTHING`
	}
	if incrPk != "" {
		ql = ql + ` RETURNING ` + formatIdents(incrPk) + ` AS "LAST_INSERT_ID"`
	}
	query = &GenericQuery{
		method:      method,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newInsertOrUpdateGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	method := dal.Execute
	incrPk := ""
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	valuesIdx := 0
	conflictColumns := make([]string, 0, 1)
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsIncrPk() {
			method = dal.Query
			incrPk = field.Column()
			continue
		}
		if field.IsAMB() || field.IsAMT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() {
			continue
		}
		if field.IsAOL() {
			column := field.Column()
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesFragments = valuesFragments + `, 1`
			continue
		}
		columns := field.Columns()
		for _, column := range columns {
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesIdx++
			valuesFragments = valuesFragments + `, ` + fmt.Sprintf("$%d", valuesIdx)
		}
		if field.Conflict() {
			conflictColumns = append(conflictColumns, columns...)
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	if len(conflictColumns) == 0 {
		return
	}
	columnsFragments = columnsFragments[2:]
	valuesFragments = valuesFragments[2:]
	ql := `INSERT INTO ` + tableName + ` ` + `(` + columnsFragments + `)` + ` VALUES (` + valuesFragments + `)`
	conflicts := ""
	for _, conflictColumn := range conflictColumns {
		conflicts = conflicts + ", " + formatIdents(conflictColumn)
	}
	conflicts = conflicts[2:]
	ql = ql + ` ON CONFLICT (` + conflicts + `) DO `
	updateFragment := ""
	for _, field := range fields {
		if field.IsPk() || field.IsIncrPk() || field.IsACB() || field.IsACT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() {
			continue
		}
		if field.IsAOL() {
			aolIdent := formatIdents(field.Column())
			updateFragment = updateFragment + ", " + aolIdent + ` = ` + aolIdent + `+1`
			continue
		}
		columns := field.Columns()
		for _, column := range columns {
			valuesIdx++
			columnIdent := formatIdents(column)
			updateFragment = updateFragment + ", " + columnIdent + ` = ` + fmt.Sprintf("$%d", valuesIdx)
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesIdx++
			valuesFragments = valuesFragments + `, ` + fmt.Sprintf("$%d", valuesIdx)
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	updateFragment = updateFragment[2:]
	ql = ql + `UPDATE SET ` + updateFragment
	if incrPk != "" {
		ql = ql + ` RETURNING ` + formatIdents(incrPk) + ` AS "LAST_INSERT_ID"`
	}
	query = &GenericQuery{
		method:      method,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newInsertWhenExistGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	query, err = newInsertWhenExistOrNotGenericQuery(structure, true)
	return
}

func newInsertWhenNotExistGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	query, err = newInsertWhenExistOrNotGenericQuery(structure, false)
	return
}

func newInsertWhenExistOrNotGenericQuery(structure *dal.ModelStructure, exist bool) (query *GenericQuery, err error) {
	method := dal.Execute
	incrPk := ""
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	valuesIdx := 0
	conflictColumns := make([]string, 0, 1)
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsIncrPk() {
			method = dal.Query
			incrPk = field.Column()
			continue
		}
		if field.IsAMB() || field.IsAMT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() {
			continue
		}
		if field.IsAOL() {
			column := field.Column()
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesFragments = valuesFragments + `, 1`
			continue
		}
		columns := field.Columns()
		for _, column := range columns {
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesIdx++
			valuesFragments = valuesFragments + `, ` + fmt.Sprintf("$%d", valuesIdx)
		}
		if field.Conflict() {
			conflictColumns = append(conflictColumns, columns...)
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	columnsFragments = columnsFragments[2:]
	valuesFragments = valuesFragments[2:]
	ql := `INSERT INTO ` + tableName + ` ` + `(` + columnsFragments + `)` + ` SELECT ` + valuesFragments + ` FROM (SELECT 1) AS "__TMP__" WHERE `
	if exist {
		ql = ql + `EXISTS`
	} else {
		ql = ql + `NOT EXISTS`
	}
	ql = ql + ` (SELECT 1 FROM (` + "$$SOURCE_QUERY$$" + `) AS "__SRC__")`
	if incrPk != "" {
		ql = ql + ` RETURNING ` + formatIdents(incrPk) + ` AS "LAST_INSERT_ID"`
	}
	query = &GenericQuery{
		method:      method,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newUpdateGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	valuesIdx := 0
	pkFields := make([]*dal.Field, 0, 1)
	var aolField *dal.Field
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	updateFragment := ""
	for _, field := range fields {
		if field.IsPk() || field.IsIncrPk() {
			pkFields = append(pkFields, field)
			continue
		}
		if field.IsACB() || field.IsACT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() {
			continue
		}
		if field.IsAOL() {
			aolField = field
			aolIdent := formatIdents(field.Column())
			updateFragment = updateFragment + ", " + aolIdent + ` = ` + aolIdent + `+1`
			continue
		}
		columns := field.Columns()
		for _, column := range columns {
			valuesIdx++
			columnIdent := formatIdents(column)
			updateFragment = updateFragment + ", " + columnIdent + ` = ` + fmt.Sprintf("$%d", valuesIdx)
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesIdx++
			valuesFragments = valuesFragments + `, ` + fmt.Sprintf("$%d", valuesIdx)
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	if len(pkFields) == 0 {
		return
	}
	updateFragment = updateFragment[2:]
	// conditions
	condFragment := ""
	for _, pk := range pkFields {
		valuesIdx++
		condFragment = condFragment + " AND " + formatIdents(pk.Column()) + " = " + fmt.Sprintf("$%d", valuesIdx)
		targetFields = append(targetFields, newGenericQueryModelFields(pk)...)
	}
	if aolField != nil {
		valuesIdx++
		condFragment = condFragment + " AND " + formatIdents(aolField.Column()) + " = " + fmt.Sprintf("$%d", valuesIdx)
		targetFields = append(targetFields, newGenericQueryModelFields(aolField)...)
	}
	condFragment = condFragment[5:]
	ql := `UPDATE ` + tableName + ` SET ` + updateFragment + ` WHERE ` + condFragment
	query = &GenericQuery{
		method:      dal.Execute,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newDeleteGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	valuesIdx := 0
	pkFields := make([]*dal.Field, 0, 1)
	var aolField *dal.Field
	var useUpdate bool
	targetFields := make([]*GenericQueryModelField, 0, 1)
	updateFragment := ""
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsPk() || field.IsIncrPk() {
			pkFields = append(pkFields, field)
			continue
		}
		if field.IsACB() || field.IsACT() || field.IsAMB() || field.IsAMT() || field.IsVirtual() || field.IsLink() {
			continue
		}
		if field.IsAOL() {
			aolField = field
			aolIdent := formatIdents(field.Column())
			updateFragment = updateFragment + ", " + aolIdent + ` = ` + aolIdent + `+1`
			continue
		}
		if field.IsADB() || field.IsADT() {
			useUpdate = true
			valuesIdx++
			column := field.Column()
			columnIdent := formatIdents(column)
			updateFragment = updateFragment + ", " + columnIdent + ` = ` + fmt.Sprintf("$%d", valuesIdx)
			columnsFragments = columnsFragments + ", " + formatIdents(column)
			valuesIdx++
			valuesFragments = valuesFragments + `, ` + fmt.Sprintf("$%d", valuesIdx)
			targetFields = append(targetFields, newGenericQueryModelFields(field)...)
		}
	}
	if len(pkFields) == 0 {
		return
	}
	// conditions
	condFragment := ""
	for _, pk := range pkFields {
		valuesIdx++
		condFragment = condFragment + " AND " + formatIdents(pk.Column()) + " = " + fmt.Sprintf("$%d", valuesIdx)
		targetFields = append(targetFields, newGenericQueryModelFields(pk)...)
	}
	if aolField != nil {
		valuesIdx++
		condFragment = condFragment + " AND " + formatIdents(aolField.Column()) + " = " + fmt.Sprintf("$%d", valuesIdx)
		targetFields = append(targetFields, newGenericQueryModelFields(aolField)...)
	}
	condFragment = condFragment[5:]
	// ql
	ql := ""
	if useUpdate {
		updateFragment = updateFragment[2:]
		ql = `UPDATE ` + tableName + ` SET ` + updateFragment + ` WHERE ` + condFragment
	} else {
		ql = `DELETE FROM ` + tableName + ` WHERE ` + condFragment
	}
	query = &GenericQuery{
		method:      dal.Execute,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newExistGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT 1 AS "_EXIST_" FROM ` + tableName
	query = &GenericQuery{
		method:      dal.Query,
		value:       ql,
		modelFields: nil,
	}
	return
}

func newCountGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT count(1) AS "_COUNT_" FROM ` + tableName
	query = &GenericQuery{
		method:      dal.Query,
		value:       ql,
		modelFields: nil,
	}
	return
}

func newSelectColumnsFragment(structure *dal.ModelStructure) (fragment string) {
	schema, name := structure.Name()
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsReference() {
			/*
				SELECT row_to_json("ref_table".*) FROM (
				SELECT ... FROM "schema"."ref_table" WHERE "pk" = "host_full_table_name"."ref_column" OFFSET 0 LIMIT 1
				) AS "ref_table"
			*/
			targetModel, _, targetColumns := field.Reference().Target()
			targetSchema, targetName := targetModel.Name()
			sqSelectsFragment := ""
			if field.Reference().Abstracted() {
				pks := make([]string, 0, 1)
				for _, targetModelField := range targetModel.Fields() {
					if targetModelField.IsPk() || targetModelField.IsIncrPk() {
						pks = append(pks, formatIdents(targetSchema, targetName, targetModelField.Column()))
					}
				}
				if len(pks) == 0 {
					sqSelectsFragment = `'{}'::jsonb`
				} else {
					sqSelectsFragment = strings.Join(pks, ", ")
				}
			} else {
				sqSelectsFragment = newSelectColumnsFragment(targetModel)
			}
			sq := `SELECT row_to_json(` + formatIdents(targetSchema, targetName) + `.*) FROM (`
			sq = sq + `SELECT ` + sqSelectsFragment + ` FROM ` + formatIdents(targetSchema, targetName) + ` WHERE `
			// cond
			sqConditionFragment := ""
			for i, targetColumn := range targetColumns {
				srcColumn := field.Columns()[i]
				sqConditionFragment = sqConditionFragment + " AND " + formatIdents(targetSchema, targetName, targetColumn) + " = " + formatIdents(schema, name, srcColumn)
			}
			sqConditionFragment = sqConditionFragment[5:]
			sq = sq + sqConditionFragment + ` OFFSET 0 LIMIT 1`
			sq = sq + `) AS ` + formatIdents(targetSchema, targetName)
			fragment = fragment + ", (" + sq + ") AS " + formatIdents(field.Reference().Name())
			continue
		}
		if field.IsLink() {
			/** one
			SELECT row_to_json("ref_table".*) FROM (
			SELECT ... FROM "schema"."ref_table" WHERE "link" = "host_full_table_name"."pk" OFFSET 0 LIMIT 1
			) AS "ref_table"
			*/
			/** array
			SELECT to_json(ARRAY(
				SELECT row_to_json("ref_table".*) FROM (
				SELECT ... FROM "schema"."ref_table" WHERE "pk" = "host_full_table_name"."ref_column" ORDER BY ... OFFSET x LIMIT y
				) AS "ref_table"
			))
			*/
			link := field.Link()
			targetModel, targetColumns, linkOrders, linkRange := link.Target()
			targetSchema, targetName := targetModel.Name()
			sqSelectsFragment := ""
			if link.Abstracted() {
				pks := make([]string, 0, 1)
				for _, targetModelField := range targetModel.Fields() {
					if targetModelField.IsPk() || targetModelField.IsIncrPk() {
						pks = append(pks, formatIdents(targetSchema, targetName, targetModelField.Column()))
					}
				}
				if len(pks) == 0 {
					sqSelectsFragment = `'{}'::jsonb`
				} else {
					sqSelectsFragment = strings.Join(pks, ", ")
				}
			} else {
				sqSelectsFragment = newSelectColumnsFragment(targetModel)
			}

			sq := `SELECT row_to_json(` + formatIdents(targetSchema, targetName) + `.*) FROM (`
			sq = sq + `SELECT ` + sqSelectsFragment + ` FROM ` + formatIdents(targetSchema, targetName) + ` WHERE `
			// cond
			sqConditionFragment := ""
			for i, targetColumn := range targetColumns {
				srcColumn := field.Columns()[i]
				sqConditionFragment = sqConditionFragment + " AND " + formatIdents(targetSchema, targetName, targetColumn) + " = " + formatIdents(schema, name, srcColumn)
			}
			sqConditionFragment = sqConditionFragment[5:]
			sq = sq + sqConditionFragment
			if linkOrders != nil {
				orderByFragments := ""
				linkOrders.Unfold(func(order *dal.Order) {
					orderByFragments = orderByFragments + ", " + formatIdents(targetSchema, targetName, order.Column)
					if order.Desc {
						orderByFragments = orderByFragments + " DESC"
					}
				})
				if orderByFragments != "" {
					sq = sq + ` ORDER BY ` + orderByFragments[2:]
				}
			}
			if linkRange == nil {
				linkRange = dal.NewRange(0, 1)
			}
			offset, limit := linkRange.Value()
			sq = sq + ` OFFSET ` + strconv.Itoa(offset) + ` LIMIT ` + strconv.Itoa(limit)
			sq = sq + `) AS ` + formatIdents(targetSchema, targetName)
			fragment = fragment + ", (SELECT to_json(ARRAY(" + sq + "))) AS " + formatIdents(field.Reference().Name())
			continue
		}
		if field.IsVirtual() {
			fragment = fragment + ", (" + field.Virtual().Query() + ") AS " + formatIdents(field.Virtual().Name())
			continue
		}
		fragment = fragment + ", " + formatIdents(schema, name, field.Column())
	}
	fragment = fragment[2:]
	return
}

func newSelectGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT ` + newSelectColumnsFragment(structure) + ` FROM ` + tableName
	query = &GenericQuery{
		method:      dal.Query,
		value:       ql,
		modelFields: nil,
	}
	return
}

func newPageGenericQuery(structure *dal.ModelStructure) (query *GenericQuery, err error) {
	// todo use one query via json
	return
}

func newGenericQueryModelFields(field *dal.Field) (v []*GenericQueryModelField) {
	v = make([]*GenericQueryModelField, 0, 1)
	if field.IsVirtual() || field.IsLink() {
		return
	}
	if field.IsReference() {
		_, fields, _ := field.Reference().Target()
		for _, f := range fields {
			v = append(v, &GenericQueryModelField{
				v: field.Name(),
				p: f.Name(),
			})
		}
	}
	v = append(v, &GenericQueryModelField{
		v: field.Name(),
		p: "",
	})
	return
}

type GenericQueryModelField struct {
	v string
	p string
}

func (field *GenericQueryModelField) Value(modelValue reflect.Value) (v interface{}) {
	fv := modelValue.FieldByName(field.v)
	if !fv.IsValid() {
		return
	}
	if field.p == "" {
		v = fv.Interface()
		return
	}
	fv = fv.FieldByName(field.p)
	if !fv.IsValid() {
		return
	}
	v = fv.Interface()
	return
}

type GenericQuery struct {
	method      dal.QueryMethod
	value       string
	modelFields []*GenericQueryModelField
}

func (generic *GenericQuery) WeaveExecute(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generic *GenericQuery) WeaveQuery(cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}
