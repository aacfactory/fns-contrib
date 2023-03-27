package mysql

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"reflect"
	"strings"
)

func formatIdents(s ...string) (v string) {
	for _, n := range s {
		n = strings.ToUpper(strings.TrimSpace(n))
		if n == "" {
			continue
		}
		v = v + ".`" + strings.ToUpper(strings.TrimSpace(n)) + "`"
	}
	if v != "" {
		v = v[1:]
	}
	return
}

func newInsertQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	conflictColumns := make([]string, 0, 1)
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsIncrPk() {
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
			valuesFragments = valuesFragments + `, ?`
		}
		if field.Conflict() {
			conflictColumns = append(conflictColumns, columns...)
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	columnsFragments = columnsFragments[2:]
	valuesFragments = valuesFragments[2:]
	ql := ""
	if len(conflictColumns) > 0 {
		ql = "INSERT IGNORE INTO " + tableName + ` ` + `(` + columnsFragments + `)` + ` VALUES (` + valuesFragments + `)`
	} else {
		ql = "INSERT INTO " + tableName + ` ` + `(` + columnsFragments + `)` + ` VALUES (` + valuesFragments + `)`
	}
	query = &GenericQuery{
		method:      dal.ExecuteMode,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newInsertOrUpdateQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	conflictColumns := make([]string, 0, 1)
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsIncrPk() {
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
			valuesFragments = valuesFragments + `, ?`
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
	ql := `INSERT INTO ` + tableName + ` ` + `(` + columnsFragments + `)` + ` VALUES (` + valuesFragments + `)` + ` ON DUPLICATE KEY `
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
			columnIdent := formatIdents(column)
			updateFragment = updateFragment + ", " + columnIdent + ` = VALUES(` + columnIdent + ")"
		}
	}
	updateFragment = updateFragment[2:]
	ql = ql + `UPDATE ` + updateFragment
	query = &GenericQuery{
		method:      dal.ExecuteMode,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newInsertWhenExistQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	query = newInsertWhenExistOrNotGenericQuery(structure, true)
	return
}

func newInsertWhenNotExistQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	query = newInsertWhenExistOrNotGenericQuery(structure, false)
	return
}

func newInsertWhenExistOrNotGenericQuery(structure *dal.ModelStructure, exist bool) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	columnsFragments := ""
	valuesFragments := ""
	targetFields := make([]*GenericQueryModelField, 0, 1)
	fields := structure.Fields()
	for _, field := range fields {
		if field.IsIncrPk() {
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
			valuesFragments = valuesFragments + `, ?`
		}
		targetFields = append(targetFields, newGenericQueryModelFields(field)...)
	}
	columnsFragments = columnsFragments[2:]
	valuesFragments = valuesFragments[2:]
	ql := `INSERT INTO ` + tableName + ` ` + `(` + columnsFragments + `)` + ` SELECT ` + valuesFragments + ` FROM (SELECT 1) AS __TMP__ WHERE `
	if exist {
		ql = ql + `EXISTS`
	} else {
		ql = ql + `NOT EXISTS`
	}
	ql = ql + ` (SELECT 1 FROM (` + "$$SOURCE_QUERY$$" + `) AS __SRC__)`
	query = &GenericQuery{
		method:      dal.ExecuteMode,
		value:       ql,
		modelFields: targetFields,
	}
	return
}

func newUpdateQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
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
			columnIdent := formatIdents(column)
			updateFragment = updateFragment + ", " + columnIdent + ` = ?`
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
		condFragment = condFragment + " AND " + formatIdents(pk.Column()) + " = ?"
		targetFields = append(targetFields, newGenericQueryModelFields(pk)...)
	}
	if aolField != nil {
		condFragment = condFragment + " AND " + formatIdents(aolField.Column()) + " = ?"
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

func newDeleteQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
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
			column := field.Column()
			columnIdent := formatIdents("", "", column)
			updateFragment = updateFragment + ", " + columnIdent + ` = ?`
		}
	}
	if len(pkFields) == 0 {
		return
	}
	// conditions
	condFragment := ""
	for _, pk := range pkFields {
		condFragment = condFragment + " AND " + formatIdents(pk.Column()) + " = ?"
		targetFields = append(targetFields, newGenericQueryModelFields(pk)...)
	}
	if aolField != nil {
		condFragment = condFragment + " AND " + formatIdents(aolField.Column()) + " = ?"
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
	ql := `SELECT 1 AS _EXIST_ FROM ` + tableName
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
	ql := `SELECT count(1) AS _COUNT_ FROM ` + tableName
	query = &GenericQuery{
		method:      dal.QueryMode,
		value:       ql,
		modelFields: nil,
	}
	return
}

func generateReferenceQuery(field *dal.Field) (query string) {
	/*
			SELECT JSON_OBJECT
		    ('id', id,
		     'name', name,
		     'age', age, 'create_at', create_at) as ref_table
		     FROM `fns-test`.user;
	*/
	hostSchema, hostTable := field.Model().Name()
	hostColumns := field.Columns()
	targetModel, _, targetColumns := field.Reference().Target()
	targetSchema, targetTable := targetModel.Name()
	targetTableName := formatIdents(targetSchema, targetTable)
	// selects
	selectsFragment := ""
	if field.Reference().Abstracted() {
		for _, targetField := range targetModel.Fields() {
			if targetField.IsPk() || targetField.IsIncrPk() {
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', %s", targetField.JsonName(), targetField.Column())
			}
		}
		if selectsFragment == "" {
			selectsFragment = ", '_NULL_', '\"\"'"
		}
		selectsFragment = selectsFragment[2:]
	} else {
		for _, targetField := range targetModel.Fields() {
			if !targetField.HasColumns() {
				continue
			}
			if targetField.IsReference() {
				subReference := generateReferenceQuery(targetField)
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', (%s)", targetField.JsonName(), subReference)
			} else if targetField.IsLink() {
				subLink := generateLinkQuery(targetField)
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', (%s)", targetField.JsonName(), subLink)
			} else if targetField.IsVirtual() {
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', (%s)", targetField.JsonName(), targetField.Virtual().Query())
			} else {
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', %s", targetField.JsonName(), targetField.Column())
			}
		}
		selectsFragment = selectsFragment[2:]
	}
	// conditions
	conditionsFragment := ""
	for i, hostColumn := range hostColumns {
		targetColumn := targetColumns[i]
		conditionsFragment = conditionsFragment + " AND " + formatIdents(targetSchema, targetTable, targetColumn) + " = " + formatIdents(hostSchema, hostTable, hostColumn)
	}
	conditionsFragment = conditionsFragment[5:]
	// query
	query = `SELECT JSON_OBJECT(` + selectsFragment + `) FROM ` + targetTableName + ` WHERE ` + conditionsFragment
	return
}

func generateLinkQuery(field *dal.Field) (query string) {
	/*
		SELECT JSON_ARRAYAGG(
			JSON_OBJECT('id', id, 'name', name, 'age', age, 'create_at', create_at)
		)
		FROM `fns-test`.`user` AS foo FROM `fns-test`.user;
	*/
	hostSchema, hostTable := field.Model().Name()
	hostColumns := field.Columns()
	targetModel, targetColumns, orders, rng := field.Link().Target()
	targetSchema, targetTable := targetModel.Name()
	targetTableName := formatIdents(targetSchema, targetTable)
	// selects
	selectsFragment := ""
	if field.Link().Abstracted() {
		for _, targetField := range targetModel.Fields() {
			if targetField.IsPk() || targetField.IsIncrPk() {
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', %s", targetField.JsonName(), targetField.Column())
			}
		}
		if selectsFragment == "" {
			selectsFragment = ", '_NULL_', '\"\"'"
		}
		selectsFragment = selectsFragment[2:]
	} else {
		for _, targetField := range targetModel.Fields() {
			if !targetField.HasColumns() {
				continue
			}
			if targetField.IsReference() {
				subReference := generateReferenceQuery(targetField)
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', (%s)", targetField.JsonName(), subReference)
			} else if targetField.IsLink() {
				subLink := generateLinkQuery(targetField)
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', (%s)", targetField.JsonName(), subLink)
			} else if targetField.IsVirtual() {
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', (%s)", targetField.JsonName(), targetField.Virtual().Query())
			} else {
				selectsFragment = selectsFragment + fmt.Sprintf(", '%s', %s", targetField.JsonName(), targetField.Column())
			}
		}
		selectsFragment = selectsFragment[2:]
	}
	selectsFragment = "JSON_OBJECT(" + selectsFragment + ")"
	if field.Link().Arrayed() {
		selectsFragment = "JSON_ARRAYAGG(" + selectsFragment + ")"
	}
	// conditions
	conditionsFragment := ""
	for i, hostColumn := range hostColumns {
		targetColumn := targetColumns[i]
		conditionsFragment = conditionsFragment + " AND " + formatIdents(targetSchema, targetTable, targetColumn) + " = " + formatIdents(hostSchema, hostTable, hostColumn)
	}
	conditionsFragment = conditionsFragment[5:]
	query = query + " WHERE " + conditionsFragment
	// orders
	ordersFragment := ""
	if orders != nil {
		ordersFragment = generateOrder(orders)

	}
	// ranges
	rangeFragment := ""
	if rng != nil {
		rangeFragment = generateRange(rng)
	}
	// query
	query = `SELECT ` + selectsFragment + ` FROM ` + targetTableName + ` WHERE ` + conditionsFragment + " " + ordersFragment + " " + rangeFragment
	return
}

func newSelectColumnsFragment(structure *dal.ModelStructure) (fragment string) {
	fields := structure.Fields()
	for _, field := range fields {
		if !field.HasColumns() {
			continue
		}
		if field.IsReference() {
			referenceQuery := generateReferenceQuery(field)
			fragment = fragment + ", (" + referenceQuery + ") AS " + field.Reference().Name()
			continue
		}
		if field.IsLink() {
			linkQuery := generateLinkQuery(field)
			fragment = fragment + ", (" + linkQuery + ") AS " + field.Link().Name()
			continue
		}
		if field.IsVirtual() {
			fragment = fragment + ", (" + field.Virtual().Query() + ") AS " + field.Virtual().Name()
			continue
		}
		if field.IsTreeType() {
			continue
		}
		fragment = fragment + ", " + formatIdents("", "", field.Column())
	}
	fragment = fragment[2:]
	return
}

func newSelectQuery(structure *dal.ModelStructure) (query *GenericQuery) {
	schema, name := structure.Name()
	tableName := formatIdents(schema, name)
	ql := `SELECT ` + newSelectColumnsFragment(structure) + ` FROM ` + tableName
	query = &GenericQuery{
		method:      dal.QueryMode,
		value:       ql,
		modelFields: nil,
	}
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
		err = errors.Warning("mysql: weave update with values failed").WithCause(errors.Warning("values is required"))
		return
	}
	method, query = generic.method, generic.value
	setIdx := strings.Index(query, " SET ")
	if setIdx < 0 {
		err = errors.Warning("mysql: weave update with values failed").WithCause(errors.Warning("`SET` was not found in query"))
		return
	}
	query = query[:setIdx+5]
	arguments = make([]interface{}, 0, 1)
	setFragment, setArgs, setErr := generateValues(values)
	if setErr != nil {
		err = errors.Warning("mysql: weave update with values failed").WithCause(setErr)
		return
	}
	query = query + setFragment
	arguments = append(arguments, setArgs...)
	if cond != nil {
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
		err = errors.Warning("mysql: weave delete with conditions failed").WithCause(errors.Warning("`WHERE` was not found in query"))
		return
	}
	query = query[:whereIdx]
	if cond != nil {
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
				argsFragment = argsFragment + ", ?"
			}
			fragment = fragment + argsFragment[2:]
		}
		fragment = fragment + ")"
		break
	case "BETWEEN":
		arguments = append(arguments, args...)
		fragment = fragment + " ? AND ?"
		break
	case "LIKE":
		fragment = fragment + " " + args[0].(string)
		break
	default:
		// json object
		if strings.Index(op, "->>") == 0 {
			subOp := op[strings.LastIndex(op, " ")+1:]
			switch subOp {
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
						argsFragment = argsFragment + ", ?"
					}
					fragment = fragment + argsFragment[2:]
				}
				fragment = fragment + ")"
				break
			case "BETWEEN":
				arguments = append(arguments, args...)
				fragment = fragment + " ? AND ?"
				break
			case "LIKE":
				fragment = fragment + " " + args[0].(string)
				break
			default:
				break
			}
		}
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
				argsFragment = argsFragment + ", ?"
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
			fragments = append(fragments, fmt.Sprintf("%s = ?", formatIdents(field)))
			arguments = append(arguments, value.Value)
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
	fragment = fmt.Sprintf("LIMIT %d,%d", offset, limit)
	return
}
