package postgres

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
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

func newInsertQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	method := dal.ExecuteMode
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
			method = dal.QueryMode
			incrPk = field.Column()
			continue
		}
		if field.IsAMB() || field.IsAMT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() || field.IsTreeType() {
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

func newInsertOrUpdateQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	method := dal.ExecuteMode
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
			method = dal.QueryMode
			incrPk = field.Column()
			continue
		}
		if field.IsAMB() || field.IsAMT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() || field.IsTreeType() {
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
		if field.IsPk() || field.IsIncrPk() || field.IsACB() || field.IsACT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() || field.IsTreeType() {
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

func newInsertWhenExistQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	query = newInsertWhenExistOrNotQuery(structure, true)
	return
}

func newInsertWhenNotExistQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	query = newInsertWhenExistOrNotQuery(structure, false)
	return
}

func newInsertWhenExistOrNotQuery(structure *dal.ModelStructure, exist bool) (query *GenericQuery) {
	method := dal.ExecuteMode
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
			method = dal.QueryMode
			incrPk = field.Column()
			continue
		}
		if field.IsAMB() || field.IsAMT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() || field.IsTreeType() {
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

func newUpdateQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
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
		if field.IsACB() || field.IsACT() || field.IsADB() || field.IsADT() || field.IsVirtual() || field.IsLink() || field.IsTreeType() {
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
		method:      dal.ExecuteMode,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newDeleteQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
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
		if field.IsACB() || field.IsACT() || field.IsAMB() || field.IsAMT() || field.IsVirtual() || field.IsLink() || field.IsTreeType() {
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
		method:      dal.ExecuteMode,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newExistQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT 1 AS "_EXIST_" FROM ` + tableName
	query = &GenericQuery{
		method:      dal.QueryMode,
		value:       ql,
		modelFields: nil,
	}
	return
}

func newCountQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT count(1) AS "_COUNT_" FROM ` + tableName
	query = &GenericQuery{
		method:      dal.QueryMode,
		value:       ql,
		modelFields: nil,
	}
	return
}

func newSelectColumnsFragment(structure *dal.ModelStructure, useJson bool) (fragment string) {
	schema, name := structure.Name()
	fields := structure.Fields()
	for _, field := range fields {
		if !field.HasColumns() {
			continue
		}
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
						pks = append(pks, fmt.Sprintf("%s AS \"%s\"", formatIdents(targetSchema, targetName, targetModelField.Column()), targetModelField.JsonName()))
					}
				}
				if len(pks) == 0 {
					sqSelectsFragment = `'{}'::jsonb`
				} else {
					sqSelectsFragment = strings.Join(pks, ", ")
				}
			} else {
				sqSelectsFragment = newSelectColumnsFragment(targetModel, true)
			}
			sq := `SELECT row_to_json(` + formatIdents(targetName) + `.*) FROM (`
			sq = sq + `SELECT ` + sqSelectsFragment + ` FROM ` + formatIdents(targetSchema, targetName) + ` WHERE `
			// cond
			sqConditionFragment := ""
			for i, targetColumn := range targetColumns {
				srcColumn := field.Columns()[i]
				sqConditionFragment = sqConditionFragment + " AND " + formatIdents(targetSchema, targetName, targetColumn) + " = " + formatIdents(schema, name, srcColumn)
			}
			sqConditionFragment = sqConditionFragment[5:]
			sq = sq + sqConditionFragment + ` OFFSET 0 LIMIT 1`
			sq = sq + `) AS ` + formatIdents(targetName)
			if useJson {
				fragment = fragment + ", (" + sq + ") AS " + `"` + field.JsonName() + `"`
			} else {
				fragment = fragment + ", (" + sq + ") AS " + formatIdents(field.Reference().Name())
			}

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
						pks = append(pks, fmt.Sprintf("%s AS \"%s\"", formatIdents(targetSchema, targetName, targetModelField.Column()), targetModelField.JsonName()))
					}
				}
				if len(pks) == 0 {
					sqSelectsFragment = `'{}'::jsonb`
				} else {
					sqSelectsFragment = strings.Join(pks, ", ")
				}
			} else {
				sqSelectsFragment = newSelectColumnsFragment(targetModel, true)
			}

			sq := `SELECT row_to_json(` + formatIdents(targetName) + `.*) FROM (`
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
			sq = sq + `) AS ` + formatIdents(targetName)
			if link.Arrayed() {
				if useJson {
					fragment = fragment + ", (SELECT to_json(ARRAY(" + sq + "))) AS " + `"` + field.JsonName() + `"`
				} else {
					fragment = fragment + ", (SELECT to_json(ARRAY(" + sq + "))) AS " + formatIdents(field.Link().Name())
				}
			} else {
				if useJson {
					fragment = fragment + ", (" + sq + ") AS " + `"` + field.JsonName() + `"`
				} else {
					fragment = fragment + ", (" + sq + ") AS " + formatIdents(field.Link().Name())
				}
			}
			continue
		}
		if field.IsVirtual() {
			if useJson {
				fragment = fragment + ", (" + field.Virtual().Query() + ") AS " + `"` + field.JsonName() + `"`
			} else {
				fragment = fragment + ", (" + field.Virtual().Query() + ") AS " + formatIdents(field.Virtual().Name())
			}
			continue
		}
		if field.IsTreeType() {
			continue
		}
		if useJson {
			fragment = fragment + ", " + formatIdents(schema, name, field.Column()) + " AS " + `"` + field.JsonName() + `"`
		} else {
			fragment = fragment + ", " + formatIdents(schema, name, field.Column())
		}
	}
	fragment = fragment[2:]
	return
}

func newSelectQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT ` + newSelectColumnsFragment(structure, false) + ` FROM ` + tableName
	query = &GenericQuery{
		method:      dal.QueryMode,
		value:       ql,
		modelFields: nil,
	}
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
		return
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
	fv = modelValue.FieldByName(field.p)
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

func (generic *GenericQuery) WeaveExecute(_ context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query = generic.method, generic.value
	arguments = make([]interface{}, 0, 1)
	rv := reflect.ValueOf(model)
	for _, field := range generic.modelFields {
		arg := field.Value(rv.Elem())
		arguments = append(arguments, arg)
	}
	return
}

func (generic *GenericQuery) WeaveUpdateWithValues(ctx context.Context, values dal.Values, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	if values == nil || len(values) == 0 {
		err = errors.Warning("postgres: weave update with values failed").WithCause(errors.Warning("values is required"))
		return
	}
	method, query = generic.method, generic.value
	setIdx := strings.Index(query, " SET ")
	if setIdx < 0 {
		err = errors.Warning("postgres: weave update with values failed").WithCause(errors.Warning("`SET` was not found in query"))
		return
	}
	query = query[:setIdx+5]
	arguments = make([]interface{}, 0, 1)
	setFragment, setArgs, setErr := generateValues(values)
	if setErr != nil {
		err = errors.Warning("postgres: weave update with values failed").WithCause(setErr)
		return
	}
	query = query + setFragment
	arguments = append(arguments, setArgs...)
	if cond != nil {
		ctx = setGenericConditionsArgumentNum(ctx, len(arguments))
		fragment, condArgs, genCondErr := generateConditions(ctx, cond)
		if genCondErr != nil {
			err = genCondErr
			return
		}
		query = query + " WHERE" + fragment
		arguments = append(arguments, condArgs...)
	}
	return
}

func (generic *GenericQuery) WeaveDeleteWithConditions(ctx context.Context, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query = generic.method, generic.value
	whereIdx := strings.Index(query, " WHERE ")
	if whereIdx < 0 {
		err = errors.Warning("postgres: weave delete with conditions failed").WithCause(errors.Warning("`WHERE` was not found in query"))
		return
	}
	query = query[:whereIdx]
	if cond != nil {
		ctx = setGenericConditionsArgumentNum(ctx, 0)
		fragment, condArgs, genCondErr := generateConditions(ctx, cond)
		if genCondErr != nil {
			err = genCondErr
			return
		}
		query = query + " WHERE" + fragment
		arguments = append(arguments, condArgs...)
	}
	return
}

func (generic *GenericQuery) WeaveQuery(ctx context.Context, cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query = generic.method, generic.value
	definedColumns, hasDefinedColumns := dal.DefinedSelectColumns(ctx)
	if hasDefinedColumns {
		fromIdx := strings.LastIndex(query, " FROM ")
		columns := ""
		for _, column := range definedColumns {
			columns = columns + ", " + formatIdents(column)
		}
		query = "SELECT " + columns[2:] + query[fromIdx:]
	}
	arguments = make([]interface{}, 0, 1)
	if cond != nil {
		ctx = setGenericConditionsArgumentNum(ctx, 0)
		fragment, condArgs, genCondErr := generateConditions(ctx, cond)
		if genCondErr != nil {
			err = genCondErr
			return
		}
		query = query + " WHERE" + fragment
		arguments = append(arguments, condArgs...)
	}
	if orders != nil {
		query = query + " " + generateOrder(orders)
	}
	if rng != nil {
		query = query + " " + generateRange(rng)

	}
	return
}

func generateCondition(ctx context.Context, cond *dal.Condition) (fragment string, arguments []interface{}, err error) {
	arguments = make([]interface{}, 0, 1)
	argsNum := getGenericConditionsArgumentNum(ctx)
	op := cond.Operator()
	fragment = formatIdents(cond.Column()) + " " + op
	args := cond.Arguments()
	sub := tryGetSubQueryArgument(args)
	switch op {
	case "IN", "NOT IN":
		fragment = fragment + " ("
		if sub != nil {
			subFragment, subArguments, subErr := sub.GenerateQueryFragment(ctx, dialect)
			if subErr != nil {
				err = subErr
				return
			}
			fragment = fragment + subFragment
			arguments = append(arguments, subArguments...)
		} else {
			argsFragment := ""
			for _, arg := range args {
				arguments = append(arguments, arg)
				num := argsNum.Incr()
				argsFragment = argsFragment + ", " + fmt.Sprintf("$%d", num)
			}
			fragment = fragment + argsFragment[2:]
		}
		fragment = fragment + ")"
		break
	case "BETWEEN":
		arguments = append(arguments, args...)
		fragment = fragment + fmt.Sprintf(" $%d", argsNum.Incr()) + " AND" + fmt.Sprintf(" $%d", argsNum.Incr())
		break
	case "LIKE":
		fragment = fragment + " " + args[0].(string)
		break
	case "?", "?|", "?&", "@>":
		fragment = fragment + " " + args[0].(string)
	default:
		if sub != nil {
			subFragment, subArguments, subErr := sub.GenerateQueryFragment(ctx, dialect)
			if subErr != nil {
				err = subErr
				return
			}
			fragment = fragment + "(" + subFragment + ")"
			arguments = append(arguments, subArguments...)
		} else {
			argsFragment := ""
			for _, arg := range args {
				arguments = append(arguments, arg)
				num := argsNum.Incr()
				argsFragment = argsFragment + ", " + fmt.Sprintf("$%d", num)
			}
			fragment = fragment + argsFragment[1:]
		}
	}
	return
}

func tryGetSubQueryArgument(args []interface{}) (sub *dal.SubQueryArgument) {
	arg := args[0]
	v, ok := arg.(*dal.SubQueryArgument)
	if !ok {
		return
	}
	sub = v
	return
}

func generateValues(values dal.Values) (fragment string, arguments []interface{}, err error) {
	arguments = make([]interface{}, 0, 1)
	fragments := make([]string, 0, 1)
	n := 1
	for _, value := range values {
		field := strings.TrimSpace(value.Field)
		if field == "" {
			err = errors.Warning("some field of value is nil")
			return
		}
		if value.Value == nil {
			err = errors.Warning("some field value of value is nil")
			return
		}
		unprepared, isUnprepared := value.Value.(*dal.UnpreparedValue)
		if isUnprepared {
			fragments = append(fragments, fmt.Sprintf("%s = %s", formatIdents(field), unprepared.Fragment))
		} else {
			fragments = append(fragments, fmt.Sprintf("%s = $%d", formatIdents(field), n))
			arguments = append(arguments, value.Value)
			n++
		}
	}
	if len(fragments) == 0 {
		err = errors.Warning("generate values failed")
		return
	}
	fragment = strings.Join(fragments, ", ")
	return
}

func generateConditions(ctx context.Context, conditions *dal.Conditions) (fragment string, arguments []interface{}, err error) {
	fragment = ""
	arguments = make([]interface{}, 0, 1)
	conditions.Unfold(
		func(condition *dal.Condition) {
			fragment0, fragmentArgs, genErr := generateCondition(ctx, condition)
			if genErr != nil {
				err = genErr
				return
			}
			fragment = fragment + " " + fragment0
			arguments = append(arguments, fragmentArgs...)
		}, func(operator string, condition *dal.Condition) {
			fragment0, fragmentArgs, genErr := generateCondition(ctx, condition)
			if genErr != nil {
				err = genErr
				return
			}
			fragment = fragment + " " + operator + " " + fragment0
			arguments = append(arguments, fragmentArgs...)
		},
		func(operator string, conditions *dal.Conditions) {
			fragment0, fragmentArgs, genErr := generateConditions(ctx, conditions)
			if genErr != nil {
				err = genErr
				return
			}
			fragment = fragment + " " + operator + " (" + fragment0 + ")"
			arguments = append(arguments, fragmentArgs...)
		},
	)
	return
}

func generateOrder(orders *dal.Orders) (fragment string) {
	orders.Unfold(func(order *dal.Order) {
		fragment = fragment + ", " + formatIdents(order.Column)
		if order.Desc {
			fragment = fragment + " DESC"
		}
	})
	if fragment == "" {
		return
	}
	fragment = "ORDER BY" + fragment[1:]
	return
}

func generateRange(rng *dal.Range) (fragment string) {
	offset, limit := rng.Value()
	fragment = fmt.Sprintf("OFFSET %d LIMIT %d", offset, limit)
	return
}
