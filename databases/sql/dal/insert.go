package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Insert(ctx context.Context, model Model) (err errors.CodeError) {
	if model == nil {
		return
	}
	rv := reflect.ValueOf(model)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("dal: insert failed").WithCause(fmt.Errorf(" for type of model is not ptr"))
		return
	}
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: insert failed").WithCause(getGeneratorErr)
		return
	}
	// audit
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, structure)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("dal: insert failed").WithCause(tryFillCreateErr)
		return
	}
	method, query, arguments, generateErr := generator.Insert(ctx, model)
	if generateErr != nil {
		err = errors.Warning("dal: insert failed").WithCause(generateErr)
		return
	}
	if method == QueryMode {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.ServiceError("dal: insert failed").WithCause(queryErr)
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = errors.ServiceError("dal: insert failed").WithCause(fmt.Errorf("get last insert id failed")).WithCause(columnErr)
			return
		}
		if !hasColumn {
			err = errors.ServiceError("dal: insert failed").WithCause(fmt.Errorf("LAST_INSERT_ID is not found in results"))
			return
		}
		if lastInsertId > 0 {
			pk, hasIncrPk := structure.IncrPk()
			if !hasIncrPk {
				err = errors.ServiceError("dal: insert failed").WithCause(fmt.Errorf("LAST_INSERT_ID is found in results but on incr pk in model"))
				return
			}
			rv.Elem().FieldByName(pk.Name()).SetInt(lastInsertId)
		}
	} else {
		affected, _, executeErr := sql.Execute(ctx, query, arguments...)
		if executeErr != nil {
			err = errors.ServiceError("dal: insert failed").WithCause(executeErr)
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAOLErr := tryFillAOLFieldExact(rv, structure, int64(1))
	if tryFillAOLErr != nil {
		err = errors.ServiceError("dal: insert failed").WithCause(tryFillAOLErr)
		return
	}
	return
}

func InsertOrUpdate(ctx context.Context, model Model) (err errors.CodeError) {
	if model == nil {
		return
	}
	rv := reflect.ValueOf(model)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("dal: insert or update failed").WithCause(fmt.Errorf(" for type of model is not ptr"))
		return
	}
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: insert or update failed").WithCause(getGeneratorErr)
		return
	}
	// audit
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, structure)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("dal: insert or update failed").WithCause(tryFillCreateErr)
		return
	}
	tryFillModifyErr := tryFillAuditModify(ctx, rv, structure)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("dal: insert or update failed").WithCause(tryFillModifyErr)
		return
	}
	method, query, arguments, generateErr := generator.InsertOrUpdate(ctx, model)
	if generateErr != nil {
		err = errors.Warning("dal: insert or update failed").WithCause(generateErr)
		return
	}
	if method == QueryMode {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.ServiceError("dal: insert or update failed").WithCause(queryErr)
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = errors.ServiceError("dal: insert or update failed").WithCause(fmt.Errorf("get last insert id failed")).WithCause(columnErr)
			return
		}
		if !hasColumn {
			err = errors.ServiceError("dal: insert or update failed").WithCause(fmt.Errorf("LAST_INSERT_ID is not found in results"))
			return
		}
		if lastInsertId > 0 {
			pk, hasIncrPk := structure.IncrPk()
			if !hasIncrPk {
				err = errors.ServiceError("dal: insert or update failed").WithCause(fmt.Errorf("LAST_INSERT_ID is found in results but on incr pk in model"))
				return
			}
			rv.Elem().FieldByName(pk.Name()).SetInt(lastInsertId)
		}
	} else {
		affected, _, executeErr := sql.Execute(ctx, query, arguments...)
		if executeErr != nil {
			err = errors.ServiceError("dal: insert or update failed").WithCause(executeErr)
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAOLErr := tryFillAOLField(rv, structure)
	if tryFillAOLErr != nil {
		err = errors.ServiceError("dal: insert or update failed").WithCause(tryFillAOLErr)
		return
	}
	return
}

func InsertWhenExist(ctx context.Context, model Model, source string) (err errors.CodeError) {
	if model == nil {
		return
	}
	rv := reflect.ValueOf(model)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("dal: insert when exist failed").WithCause(fmt.Errorf(" for type of model is not ptr"))
		return
	}
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: insert when exist failed").WithCause(getGeneratorErr)
		return
	}
	// audit
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, structure)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("dal: insert when exist failed").WithCause(tryFillCreateErr)
		return
	}
	method, query, arguments, generateErr := generator.InsertWhenExist(ctx, model, source)
	if generateErr != nil {
		err = errors.Warning("dal: insert when exist failed").WithCause(generateErr)
		return
	}
	if method == QueryMode {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.ServiceError("dal: insert when exist failed").WithCause(queryErr)
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = errors.ServiceError("dal: insert when exist failed").WithCause(fmt.Errorf("get last insert id failed")).WithCause(columnErr)
			return
		}
		if !hasColumn {
			err = errors.ServiceError("dal: insert when exist failed").WithCause(fmt.Errorf("LAST_INSERT_ID is not found in results"))
			return
		}
		if lastInsertId > 0 {
			pk, hasIncrPk := structure.IncrPk()
			if !hasIncrPk {
				err = errors.ServiceError("dal: insert when exist failed").WithCause(fmt.Errorf("LAST_INSERT_ID is found in results but on incr pk in model"))
				return
			}
			rv.Elem().FieldByName(pk.Name()).SetInt(lastInsertId)
		}
	} else {
		affected, _, executeErr := sql.Execute(ctx, query, arguments...)
		if executeErr != nil {
			err = errors.ServiceError("dal: insert when exist failed").WithCause(executeErr)
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAOLErr := tryFillAOLFieldExact(rv, structure, int64(1))
	if tryFillAOLErr != nil {
		err = errors.ServiceError("dal: insert when exist failed").WithCause(tryFillAOLErr)
		return
	}
	return
}

func InsertWhenNotExist(ctx context.Context, model Model, source string) (err errors.CodeError) {
	if model == nil {
		return
	}
	rv := reflect.ValueOf(model)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("dal: insert when not exist failed").WithCause(fmt.Errorf(" for type of model is not ptr"))
		return
	}
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: insert when not exist failed").WithCause(getGeneratorErr)
		return
	}
	// audit
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, structure)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("dal: insert when not exist failed").WithCause(tryFillCreateErr)
		return
	}
	method, query, arguments, generateErr := generator.InsertWhenNotExist(ctx, model, source)
	if generateErr != nil {
		err = errors.Warning("dal: insert when exist not failed").WithCause(generateErr)
		return
	}
	if method == QueryMode {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.ServiceError("dal: insert when not exist failed").WithCause(queryErr)
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = errors.ServiceError("dal: insert when not exist failed").WithCause(fmt.Errorf("get last insert id failed")).WithCause(columnErr)
			return
		}
		if !hasColumn {
			err = errors.ServiceError("dal: insert when not exist failed").WithCause(fmt.Errorf("LAST_INSERT_ID is not found in results"))
			return
		}
		if lastInsertId > 0 {
			pk, hasIncrPk := structure.IncrPk()
			if !hasIncrPk {
				err = errors.ServiceError("dal: insert when not exist failed").WithCause(fmt.Errorf("LAST_INSERT_ID is found in results but on incr pk in model"))
				return
			}
			rv.Elem().FieldByName(pk.Name()).SetInt(lastInsertId)
		}
	} else {
		affected, _, executeErr := sql.Execute(ctx, query, arguments...)
		if executeErr != nil {
			err = errors.ServiceError("dal: insert when not exist failed").WithCause(executeErr)
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAOLErr := tryFillAOLFieldExact(rv, structure, int64(1))
	if tryFillAOLErr != nil {
		err = errors.ServiceError("dal: insert when not exist failed").WithCause(tryFillAOLErr)
		return
	}
	return
}
