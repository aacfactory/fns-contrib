package sql

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/internal"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
)

const (
	name                  = "sql"
	beginTransactionFn    = "begin_transaction"
	commitTransactionFn   = "commit_transaction"
	rollbackTransactionFn = "rollback_transaction"
	queryFn               = "query"
	executeFn             = "execute"
)

func Service() service.Service {
	return &_service_{}
}

type _service_ struct {
	log logs.Logger
	dbs map[string]internal.Database
}

func (svc *_service_) Build(options service.Options) (err error) {
	svc.log = options.Log
	config := internal.Config(make(map[string]internal.DatabaseConfig))
	_, hasDriver := options.Config.Node("driver")
	if hasDriver {
		databaseConfig := internal.DatabaseConfig{}
		configErr := options.Config.As(&databaseConfig)
		if configErr != nil {
			err = errors.Warning("sql: build service failed").WithCause(configErr)
			return
		}
		config["default"] = databaseConfig
	} else {
		configErr := options.Config.As(&config)
		if configErr != nil {
			err = errors.Warning("sql: build service failed").WithCause(configErr)
			return
		}
	}
	svc.dbs, err = internal.New(internal.Options{
		Log:    options.Log,
		Config: config,
	})
	if err != nil {
		err = errors.Warning("sql: build service failed").WithCause(err)
		return
	}
	return
}

func (svc *_service_) Name() string {
	return name
}

func (svc *_service_) Internal() bool {
	return true
}

func (svc *_service_) Components() (components map[string]service.Component) {
	return
}

func (svc *_service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	targetDB := &databaseArgument{}
	targetDBErr := argument.As(&targetDB)
	if targetDBErr != nil {
		err = errors.BadRequest("sql: invalid query argument").WithCause(targetDBErr).WithMeta("service", name).WithMeta("fn", fn)
		return
	}
	if targetDB.Database == "" {
		targetDB.Database = "default"
	}
	db, hasDB := svc.dbs[targetDB.Database]
	if !hasDB {
		err = errors.ServiceError(fmt.Sprintf("sql: %s db is not found", targetDB.Database)).WithMeta("service", name).WithMeta("fn", fn)
		return
	}
	switch fn {
	case beginTransactionFn:
		appId := service.GetApplicationId(ctx)
		handleErr := db.BeginTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &transactionRegistration{
			Id: appId,
		}
		break
	case commitTransactionFn:
		finished, handleErr := db.CommitTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &transactionStatus{
			Finished: finished,
		}
		break
	case rollbackTransactionFn:
		handleErr := db.RollbackTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &service.Empty{}
		break
	case queryFn:
		qa := queryArgument{}
		argumentErr := argument.As(&qa)
		if argumentErr != nil {
			err = errors.BadRequest("sql: invalid query argument").WithCause(argumentErr).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		var queryArgs []interface{}
		if qa.Args != nil && qa.Args.Size() > 0 {
			queryArgs = qa.Args.MapToSQLArgs()
		}
		rows0, queryErr := db.Query(ctx, qa.Query, queryArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: query failed").WithCause(queryErr).WithMeta("service", name).WithMeta("fn", fn).WithMeta("query", qa.Query)
			return
		}
		result, resultErr := newRows(rows0)
		if resultErr != nil {
			err = errors.ServiceError("sql: query failed").WithCause(resultErr).WithMeta("service", name).WithMeta("fn", fn).WithMeta("query", qa.Query)
			return
		}
		v = result
		break
	case executeFn:
		ea := executeArgument{}
		argumentErr := argument.As(&ea)
		if argumentErr != nil {
			err = errors.BadRequest("sql: invalid execute argument").WithCause(argumentErr).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		var executeArgs []interface{}
		if ea.Args != nil && ea.Args.Size() > 0 {
			executeArgs = ea.Args.MapToSQLArgs()
		}
		result, queryErr := db.Execute(ctx, ea.Query, executeArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: execute failed").WithCause(queryErr).WithMeta("service", name).WithMeta("fn", fn).WithMeta("query", ea.Query)
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
		err = errors.NotFound("sql: fn was not found").WithMeta("service", name).WithMeta("fn", fn)
		break
	}
	return
}

func (svc *_service_) Close() {
	for dbname, db := range svc.dbs {
		db.Close()
		if svc.log.DebugEnabled() {
			svc.log.Debug().Caller().Message(fmt.Sprintf("sql: close %s succeed", dbname))
		}
	}
	if svc.log.DebugEnabled() {
		svc.log.Debug().Caller().Message("sql: close succeed")
	}
	return
}

func (svc *_service_) Document() (doc service.Document) {
	return
}
