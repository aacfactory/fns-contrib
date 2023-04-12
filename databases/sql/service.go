package sql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/internal"
	"github.com/aacfactory/fns/service"
)

const (
	name                  = "sql"
	beginTransactionFn    = "begin_transaction"
	commitTransactionFn   = "commit_transaction"
	rollbackTransactionFn = "rollback_transaction"
	queryFn               = "query"
	executeFn             = "execute"
	databaseDialectFn     = "database_dialect"
)

func Service(databases ...string) service.Service {
	defaultDatabaseName := ""
	components := make([]service.Component, 0, 1)
	if databases == nil || len(databases) == 0 {
		drivers := internal.RegisteredDrivers()
		if drivers == nil || len(drivers) == 0 {
			panic("sql: no sql driver was registered")
			return nil
		}
		for _, driver := range drivers {
			components = append(components, internal.New(driver))
		}
		if len(drivers) == 1 {
			defaultDatabaseName = drivers[0]
		}
	} else {
		for _, database := range databases {
			components = append(components, internal.New(database))
		}
		if len(databases) == 1 {
			defaultDatabaseName = databases[0]
		}
	}
	return &service_{
		Abstract:            service.NewAbstract(name, true, components...),
		defaultDatabaseName: defaultDatabaseName,
	}
}

type service_ struct {
	service.Abstract
	defaultDatabaseName string
}

func (svc *service_) getDatabase(name string) (db internal.Database, err errors.CodeError) {
	if name == "" && svc.defaultDatabaseName == "" {
		return
	}
	if name == "" {
		name = svc.defaultDatabaseName
		return
	}
	component, exist := svc.Components()[name]
	if !exist {
		err = errors.Warning("sql: database was not found").WithMeta("database", name)
		return
	}
	db = component.(internal.Database)
	return
}

func (svc *service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	switch fn {
	case databaseDialectFn:
		arg := dialectArgument{}
		argErr := argument.As(&arg)
		if argErr != nil {
			err = errors.BadRequest("sql: parse argument failed").WithCause(argErr)
			return
		}
		db, dbErr := svc.getDatabase(arg.Database)
		if dbErr != nil {
			err = dbErr
			return
		}
		v = &dialectResult{
			Dialect: db.Dialect(),
		}
		return
	case beginTransactionFn:
		arg := transactionBeginArgument{}
		argErr := argument.As(&arg)
		if argErr != nil {
			err = errors.BadRequest("sql: parse argument failed").WithCause(argErr)
			return
		}
		db, dbErr := svc.getDatabase(arg.Database)
		if dbErr != nil {
			err = dbErr
			return
		}
		beginErr := db.BeginTransaction(ctx)
		if beginErr != nil {
			err = errors.ServiceError("sql: begin transaction failed").WithCause(beginErr).WithMeta("database", db.Name())
			return
		}
		v = &transactionRegistration{
			Id: svc.AppId(),
		}
		break
	case commitTransactionFn:
		arg := transactionCommitArgument{}
		argErr := argument.As(&arg)
		if argErr != nil {
			err = errors.BadRequest("sql: parse argument failed").WithCause(argErr)
			return
		}
		db, dbErr := svc.getDatabase(arg.Database)
		if dbErr != nil {
			err = dbErr
			return
		}
		finished, commitErr := db.CommitTransaction(ctx)
		if commitErr != nil {
			err = errors.ServiceError("sql: commit transaction failed").WithCause(commitErr).WithMeta("database", db.Name())
			return
		}
		v = &transactionStatus{
			Finished: finished,
		}
		break
	case rollbackTransactionFn:
		arg := transactionRollbackArgument{}
		argErr := argument.As(&arg)
		if argErr != nil {
			err = errors.BadRequest("sql: parse argument failed").WithCause(argErr)
			return
		}
		db, dbErr := svc.getDatabase(arg.Database)
		if dbErr != nil {
			err = dbErr
			return
		}
		rollbackErr := db.RollbackTransaction(ctx)
		if rollbackErr != nil {
			err = errors.ServiceError("sql: rollback transaction failed").WithCause(rollbackErr).WithMeta("database", db.Name())
			return
		}
		v = &service.Empty{}
		break
	case queryFn:
		arg := queryArgument{}
		argErr := argument.As(&arg)
		if argErr != nil {
			err = errors.BadRequest("sql: parse argument failed").WithCause(argErr)
			return
		}
		db, dbErr := svc.getDatabase(arg.Database)
		if dbErr != nil {
			err = dbErr
			return
		}
		var queryArgs []interface{}
		if arg.Args != nil && arg.Args.Size() > 0 {
			queryArgs = arg.Args.Values()
		}
		rows0, queryErr := db.Query(ctx, arg.Query, queryArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: query failed").WithCause(queryErr).WithMeta("query", arg.Query).WithMeta("database", db.Name())
			return
		}
		result, resultErr := newRows(rows0)
		if resultErr != nil {
			err = errors.ServiceError("sql: query failed").WithCause(resultErr).WithMeta("query", arg.Query).WithMeta("database", db.Name())
			return
		}
		v = result
		break
	case executeFn:
		arg := executeArgument{}
		argErr := argument.As(&arg)
		if argErr != nil {
			err = errors.BadRequest("sql: parse argument failed").WithCause(argErr)
			return
		}
		db, dbErr := svc.getDatabase(arg.Database)
		if dbErr != nil {
			err = dbErr
			return
		}
		var executeArgs []interface{}
		if arg.Args != nil && arg.Args.Size() > 0 {
			executeArgs = arg.Args.Values()
		}
		result, queryErr := db.Execute(ctx, arg.Query, executeArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: execute failed").WithCause(queryErr).WithMeta("query", arg.Query).WithMeta("database", db.Name())
			return
		}
		affected, _ := result.RowsAffected()
		lastInsertId, _ := result.LastInsertId()
		v = &executeResult{
			Affected:     affected,
			LastInsertId: lastInsertId,
		}
		break
	default:
		err = errors.NotFound("sql: fn was not found")
		break
	}
	return
}
