package sql

import (
	"context"
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
	db  internal.Database
}

func (svc *_service_) Build(options service.Options) (err error) {
	svc.log = options.Log
	config := internal.Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("sql: build service failed").WithCause(configErr)
		return
	}
	svc.db, err = internal.New(internal.Options{
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
	switch fn {
	case beginTransactionFn:
		appId := service.GetAppId(ctx)
		handleErr := svc.db.BeginTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &transactionRegistration{
			Id: appId,
		}
		break
	case commitTransactionFn:
		finished, handleErr := svc.db.CommitTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &transactionStatus{
			Finished: finished,
		}
		break
	case rollbackTransactionFn:
		handleErr := svc.db.RollbackTransaction(ctx)
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
			queryArgs = qa.Args.mapToSQLArgs()
		}
		rows, queryErr := svc.db.Query(ctx, qa.Query, queryArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: query argument").WithCause(queryErr).WithMeta("service", name).WithMeta("fn", fn).WithMeta("query", qa.Query)
			return
		}
		result, resultErr := newRows(rows)
		if resultErr != nil {
			err = errors.ServiceError("sql: query argument").WithCause(resultErr).WithMeta("service", name).WithMeta("fn", fn).WithMeta("query", qa.Query)
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
			executeArgs = ea.Args.mapToSQLArgs()
		}
		result, queryErr := svc.db.Execute(ctx, ea.Query, executeArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: execute argument").WithCause(queryErr).WithMeta("service", name).WithMeta("fn", fn).WithMeta("query", ea.Query)
			return
		}
		affected, _ := result.RowsAffected()
		lastInsertId, _ := result.LastInsertId()
		v = &ExecuteResult{
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
	svc.db.Close()
	if svc.log.DebugEnabled() {
		svc.log.Debug().Caller().Message("service: close")
	}
	return
}

func (svc *_service_) Document() (doc service.Document) {

	return
}
