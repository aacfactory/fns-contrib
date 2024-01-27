package sql

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/logs"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"unsafe"
)

var (
	transactionContextKey = []byte("@fns:sql:transaction")
)

func withTransaction(ctx context.Context, tx *transactions.Transaction) {
	ctx.SetLocalValue(transactionContextKey, tx)
}

func loadTransaction(ctx context.Context) (tx *transactions.Transaction, has bool) {
	tx, has = context.LocalValue[*transactions.Transaction](ctx, transactionContextKey)
	return
}

func removeTransaction(ctx context.Context) {
	if _, has := loadTransaction(ctx); has {
		ctx.RemoveLocalValue(transactionContextKey)
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	transactionInfoContextKey = []byte("sql_transaction_info")
)

type transactionInfo struct {
	Id         string `json:"id" avro:"id"`
	EndpointId string `json:"endpointId" avro:"endpointId"`
}

func withTransactionInfo(ctx context.Context, info transactionInfo) {
	ctx.SetUserValue(transactionInfoContextKey, info)
}

func loadTransactionInfo(ctx context.Context) (info transactionInfo, has bool, err error) {
	info, has, err = context.UserValue[transactionInfo](ctx, transactionInfoContextKey)
	if err != nil {
		err = errors.Warning("sql: load transaction info from context failed").WithCause(fmt.Errorf("sql_transaction_info is not transaction info"))
		return
	}
	return
}

func removeTransactionInfo(ctx context.Context) {
	if ctx.UserValue(transactionInfoContextKey) != nil {
		ctx.RemoveUserValue(transactionInfoContextKey)
	}
}

// +-------------------------------------------------------------------------------------------------------------------+

func WithIsolation(isolation databases.Isolation) databases.TransactionOption {
	return func(options *databases.TransactionOptions) {
		options.Isolation = isolation
	}
}

func WithTransactionId(id string) databases.TransactionOption {
	return func(options *databases.TransactionOptions) {
		options.Id = bytex.FromString(id)
	}
}

func Readonly() databases.TransactionOption {
	return func(options *databases.TransactionOptions) {
		options.Readonly = true
	}
}

func Begin(ctx context.Context, options ...databases.TransactionOption) (err error) {
	r, hasRequest := services.TryLoadRequest(ctx)
	if !hasRequest {
		err = errors.Warning("sql: begin transaction failed").WithCause(fmt.Errorf("there is no request in context"))
		return
	}
	opt := databases.TransactionOptions{}
	for _, option := range options {
		option(&opt)
	}
	id := opt.Id
	if len(id) == 0 {
		id = r.Header().RequestId()
	}
	param := transactionBeginParam{
		Readonly:  opt.Readonly,
		Isolation: opt.Isolation,
		Id:        id,
		ProcessId: r.Header().ProcessId(),
	}
	eps := runtime.Endpoints(ctx)
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	response, handleErr := eps.Request(ctx, ep, transactionBeginFnName, param)
	if handleErr != nil {
		err = handleErr
		return
	}
	address, responseErr := services.ValueOfResponse[transactionAddress](response)
	if responseErr != nil {
		err = errors.Warning("sql: begin transaction failed").WithCause(responseErr)
		return
	}
	if _, hasInfo, _ := loadTransactionInfo(ctx); !hasInfo {
		// with info
		withTransactionInfo(ctx, transactionInfo{
			Id:         address.Id,
			EndpointId: address.EndpointId,
		})
	}
	if address.tx != nil {
		// with tx
		withTransaction(ctx, address.tx)
		// with debug
		if address.Debug {
			useDebugLog(ctx)
		}
	}
	return
}

type transactionAddress struct {
	Id         string                    `json:"id" avro:"id"`
	EndpointId string                    `json:"endpointId" avro:"endpointId"`
	Debug      bool                      `json:"debug" avro:"debug"`
	Reused     bool                      `json:"reused" avro:"reused"`
	tx         *transactions.Transaction `avro:"-"`
}

var (
	transactionBeginFnName = []byte("begin")
)

type transactionBeginParam struct {
	Readonly  bool                `json:"readonly" avro:"readonly"`
	Isolation databases.Isolation `json:"isolation" avro:"isolation"`
	Id        []byte              `json:"id" avro:"id"`
	ProcessId []byte              `json:"processId" avro:"processId"`
}

type transactionBeginFn struct {
	debug      bool
	endpointId string
	isolation  databases.Isolation
	db         databases.Database
	group      *transactions.Group
}

func (fn *transactionBeginFn) Name() string {
	return string(transactionBeginFnName)
}

func (fn *transactionBeginFn) Internal() bool {
	return true
}

func (fn *transactionBeginFn) Readonly() bool {
	return false
}

func (fn *transactionBeginFn) Handle(r services.Request) (v interface{}, err error) {
	param, paramErr := services.ValueOfParam[transactionBeginParam](r.Param())
	if paramErr != nil {
		err = errors.Warning("sql: begin transaction failed").WithCause(paramErr)
		return
	}
	if len(param.Id) == 0 {
		err = errors.Warning("sql: begin transaction failed").WithCause(fmt.Errorf("request id is required"))
		return
	}
	if len(param.ProcessId) == 0 {
		err = errors.Warning("sql: begin transaction failed").WithCause(fmt.Errorf("process id is required"))
		return
	}
	tx, has := fn.group.Get(param.Id)
	if has {
		acquireErr := tx.Acquire()
		if acquireErr != nil {
			err = errors.Warning("sql: begin transaction failed").WithCause(acquireErr)
			return
		}
		v = transactionAddress{
			Id:         unsafe.String(unsafe.SliceData(param.Id), len(param.Id)),
			EndpointId: fn.endpointId,
			Debug:      fn.debug,
			Reused:     true,
			tx:         tx,
		}
		return
	}

	if param.Isolation == 0 {
		param.Isolation = fn.isolation
	}
	value, beginErr := fn.db.Begin(context.TODO(), databases.TransactionOptions{
		Isolation: param.Isolation,
		Readonly:  param.Readonly,
	})
	if beginErr != nil {
		err = errors.Warning("sql: begin transaction failed").WithCause(beginErr)
		return
	}
	tx, has = fn.group.Set(param.Id, param.ProcessId, value)
	if !has {
		err = errors.Warning("sql: begin transaction failed").WithCause(fmt.Errorf("maybe duplicate begon"))
		return
	}
	v = transactionAddress{
		Id:         unsafe.String(unsafe.SliceData(param.Id), len(param.Id)),
		EndpointId: fn.endpointId,
		Debug:      fn.debug,
		Reused:     false,
		tx:         tx,
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	transactionCommitFnName = []byte("commit")
)

func Commit(ctx context.Context) (err error) {
	info, hasInfo, loadInfoErr := loadTransactionInfo(ctx)
	if loadInfoErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(loadInfoErr)
		return
	}
	if !hasInfo {
		err = errors.Warning("sql: commit transaction failed").WithCause(fmt.Errorf("transaction maybe not begin"))
		return
	}
	eps := runtime.Endpoints(ctx)
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	response, handleErr := eps.Request(ctx, ep, transactionCommitFnName, transactionCommitParam{
		Id: info.Id,
	})
	if handleErr != nil {
		err = handleErr
		return
	}

	committed, responseErr := services.ValueOfResponse[int](response)
	if responseErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(responseErr)
		return
	}

	log := logs.Load(ctx)
	switch committed {
	case 1:
		// remove transaction when pid is same
		if r, hasRequest := services.TryLoadRequest(ctx); hasRequest {
			if tx, hasTx := loadTransaction(ctx); hasTx && bytes.Equal(tx.ProcessId(), r.Header().ProcessId()) {
				removeTransaction(ctx)
			}
		}
		if log != nil && log.DebugEnabled() {
			log.Debug().With("transaction", "commit").Caller().Message(fmt.Sprintf("sql: unknown transaction commit status"))
		}
		break
	case 2:
		removeTransactionInfo(ctx)
		removeTransaction(ctx)
		if log != nil && log.DebugEnabled() {
			log.Debug().With("transaction", "commit").Caller().Message(fmt.Sprintf("sql: transaction committed"))
		}
		break
	default:
		if log != nil && log.WarnEnabled() {
			log.Warn().With("transaction", "commit").Caller().Message(fmt.Sprintf("sql: transaction holdon"))
		}
		break
	}
	return
}

type transactionCommitParam struct {
	Id string `json:"id" avro:"id"`
}

type transactionCommitFn struct {
	endpointId string
	db         databases.Database
	group      *transactions.Group
}

func (fn *transactionCommitFn) Name() string {
	return string(transactionCommitFnName)
}

func (fn *transactionCommitFn) Internal() bool {
	return true
}

func (fn *transactionCommitFn) Readonly() bool {
	return false
}

func (fn *transactionCommitFn) Handle(r services.Request) (v interface{}, err error) {
	param, paramErr := services.ValueOfParam[transactionCommitParam](r.Param())
	if paramErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(paramErr)
		return
	}
	tid := bytex.FromString(param.Id)
	tx, hasTx := fn.group.Get(tid)
	if !hasTx {
		err = errors.Warning("sql: commit transaction failed").WithCause(fmt.Errorf("transaction was timeout"))
		return
	}
	cmtErr := tx.Commit()
	if cmtErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(cmtErr)
		return
	}
	v = 1
	if tx.Closed() {
		v = 2
		fn.group.Remove(tid)
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	transactionRollbackFnName = []byte("rollback")
)

func Rollback(ctx context.Context) {
	log := logs.Load(ctx)
	// load info
	info, hasInfo, loadInfoErr := loadTransactionInfo(ctx)
	if loadInfoErr != nil {
		if log != nil && log.DebugEnabled() {
			log.Debug().With("transaction", "rollback").Cause(loadInfoErr).Caller().Message(fmt.Sprintf("sql: transaction rollback failed"))
		}
		return
	}
	if !hasInfo {
		return
	}
	eps := runtime.Endpoints(ctx)
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	param := transactionRollbackParam{
		Id: info.Id,
	}
	_, handleErr := eps.Request(ctx, ep, transactionRollbackFnName, param, services.WithEndpointId(bytex.FromString(info.EndpointId)))
	if handleErr != nil {
		if log != nil && log.DebugEnabled() {
			log.Debug().With("transaction", "rollback").Cause(handleErr).Caller().Message(fmt.Sprintf("sql: transaction rollback failed"))
		}
		return
	}
	// remove info
	removeTransactionInfo(ctx)
	// remove tx
	removeTransaction(ctx)
}

type transactionRollbackParam struct {
	Id string `json:"id" avro:"id"`
}

type transactionRollbackFn struct {
	endpointId string
	db         databases.Database
	group      *transactions.Group
}

func (fn *transactionRollbackFn) Name() string {
	return string(transactionRollbackFnName)
}

func (fn *transactionRollbackFn) Internal() bool {
	return true
}

func (fn *transactionRollbackFn) Readonly() bool {
	return false
}

func (fn *transactionRollbackFn) Handle(r services.Request) (v interface{}, err error) {
	param, paramErr := services.ValueOfParam[transactionRollbackParam](r.Param())
	if paramErr != nil {
		err = errors.Warning("sql: rollback transaction failed").WithCause(paramErr)
		return
	}
	tid := bytex.FromString(param.Id)
	tx, hasTx := fn.group.GetAndRemove(tid)
	if !hasTx {
		err = errors.Warning("sql: rollback transaction failed").WithCause(fmt.Errorf("transaction was timeout"))
		return
	}
	rbErr := tx.Rollback()
	if rbErr != nil {
		err = errors.Warning("sql: rollback transaction failed").WithCause(rbErr)
		return
	}
	return
}
