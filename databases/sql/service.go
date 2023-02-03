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
	databaseDialectFn     = "database_dialect"
)

type Option func(*Options)

type Options struct {
	name string
}

func defaultOptions() *Options {
	return &Options{
		name: name,
	}
}

func Name(name string) Option {
	return func(options *Options) {
		options.name = name
	}
}

func Service(options ...Option) service.Service {
	opt := defaultOptions()
	if options != nil {
		for _, option := range options {
			option(opt)
		}
	}
	return &_service_{
		name: opt.name,
	}
}

type _service_ struct {
	name string
	log  logs.Logger
	db   internal.Database
}

func (svc *_service_) Build(options service.Options) (err error) {
	svc.log = options.Log
	config := &internal.Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("sql: build service failed").WithCause(configErr).WithMeta("database", svc.name)
		return
	}
	svc.db, err = internal.New(internal.Options{
		Log:     options.Log,
		Config:  config,
		Barrier: options.Barrier,
	})
	if err != nil {
		err = errors.Warning("sql: build service failed").WithCause(err).WithMeta("database", svc.name)
		return
	}
	return
}

func (svc *_service_) Name() string {
	return svc.name
}

func (svc *_service_) Internal() bool {
	return true
}

func (svc *_service_) Components() (components map[string]service.Component) {
	return
}

func (svc *_service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	db := svc.db
	switch fn {
	case databaseDialectFn:
		v = &databaseInfo{
			Dialect: db.Dialect(),
		}
		return
	case beginTransactionFn:
		appId := service.GetApplicationId(ctx)
		handleErr := db.BeginTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", svc.name).WithMeta("fn", fn)
			return
		}
		v = &transactionRegistration{
			Id: appId,
		}
		break
	case commitTransactionFn:
		finished, handleErr := db.CommitTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", svc.name).WithMeta("fn", fn)
			return
		}
		v = &transactionStatus{
			Finished: finished,
		}
		break
	case rollbackTransactionFn:
		handleErr := db.RollbackTransaction(ctx)
		if handleErr != nil {
			err = handleErr.WithMeta("service", svc.name).WithMeta("fn", fn)
			return
		}
		v = &service.Empty{}
		break
	case queryFn:
		qa := queryArgument{}
		argumentErr := argument.As(&qa)
		if argumentErr != nil {
			err = errors.BadRequest("sql: invalid query argument").WithCause(argumentErr).WithMeta("service", svc.name).WithMeta("fn", fn)
			return
		}
		var queryArgs []interface{}
		if qa.Args != nil && qa.Args.Size() > 0 {
			queryArgs = qa.Args.MapToSQLArgs()
		}
		rows0, queryErr := db.Query(ctx, qa.Query, queryArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: query failed").WithCause(queryErr).WithMeta("service", svc.name).WithMeta("fn", fn).WithMeta("query", qa.Query)
			return
		}
		result, resultErr := newRows(rows0)
		if resultErr != nil {
			err = errors.ServiceError("sql: query failed").WithCause(resultErr).WithMeta("service", svc.name).WithMeta("fn", fn).WithMeta("query", qa.Query)
			return
		}
		v = result
		break
	case executeFn:
		ea := executeArgument{}
		argumentErr := argument.As(&ea)
		if argumentErr != nil {
			err = errors.BadRequest("sql: invalid execute argument").WithCause(argumentErr).WithMeta("service", svc.name).WithMeta("fn", fn)
			return
		}
		var executeArgs []interface{}
		if ea.Args != nil && ea.Args.Size() > 0 {
			executeArgs = ea.Args.MapToSQLArgs()
		}
		result, queryErr := db.Execute(ctx, ea.Query, executeArgs)
		if queryErr != nil {
			err = errors.ServiceError("sql: execute failed").WithCause(queryErr).WithMeta("service", svc.name).WithMeta("fn", fn).WithMeta("query", ea.Query)
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
		err = errors.NotFound("sql: fn was not found").WithMeta("service", svc.name).WithMeta("fn", fn)
		break
	}
	return
}

func (svc *_service_) Close() {
	svc.db.Close()
	if svc.log.DebugEnabled() {
		svc.log.Debug().Caller().With("service", svc.name).Message("sql: close succeed")
	}
	return
}

func (svc *_service_) Document() (doc service.Document) {
	return
}
