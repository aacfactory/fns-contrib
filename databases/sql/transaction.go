package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/uid"
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

func loadTransaction(ctx context.Context) (tx *transactions.Transaction, has bool, err error) {
	tx, has, err = context.LocalValue[*transactions.Transaction](ctx, transactionContextKey)
	if err != nil {
		err = errors.Warning("sql: load transaction from context failed").WithCause(fmt.Errorf("@fns:sql:transaction is not transaction"))
		return
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	transactionInfoContextKey = []byte("sql_transaction_info")
)

type transactionInfo struct {
	Id         string `json:"id"`
	EndpointId string `json:"endpointId"`
	Origin     string `json:"origin"`
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

// +-------------------------------------------------------------------------------------------------------------------+

func WithIsolation(isolation databases.Isolation) databases.TransactionOption {
	return func(options *databases.TransactionOptions) {
		options.Isolation = isolation
	}
}

func Readonly(isolation databases.Isolation) databases.TransactionOption {
	return func(options *databases.TransactionOptions) {
		options.Isolation = isolation
	}
}

func Begin(ctx context.Context, options ...databases.TransactionOption) (err error) {
	_, has, loadErr := loadTransactionInfo(ctx)
	if loadErr != nil {
		err = errors.Warning("sql: begin transaction failed").WithCause(loadErr)
		return
	}
	if has {
		return
	}
	r, ok := services.TryLoadRequest(ctx)
	if !ok {
		err = errors.Warning("sql: begin transaction failed").WithCause(fmt.Errorf("context is not endpoint request"))
		return
	}
	opt := databases.TransactionOptions{}
	for _, option := range options {
		option(&opt)
	}
	param := transactionBeginParam{
		Readonly:  opt.Readonly,
		Isolation: opt.Isolation,
	}
	eps := runtime.Endpoints(ctx)
	ep := endpointName
	if epn := loadEndpointName(ctx); len(epn) > 0 {
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
	// with info
	pid := r.Header().ProcessId()
	withTransactionInfo(ctx, transactionInfo{
		Id:         address.Id,
		EndpointId: address.EndpointId,
		Origin:     unsafe.String(unsafe.SliceData(pid), len(pid)),
	})
	return
}

type transactionAddress struct {
	Id         string `json:"id"`
	EndpointId string `json:"endpointId"`
}

var (
	transactionBeginFnName = []byte("begin")
)

type transactionBeginParam struct {
	Readonly  bool                `json:"readonly"`
	Isolation databases.Isolation `json:"isolation"`
}

type transactionBeginFn struct {
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
	tid := r.Header().RequestId()
	if len(tid) == 0 {
		tid = uid.Bytes()
	}
	tx, has := fn.group.Get(tid)
	if has {
		acquireErr := tx.Acquire()
		if acquireErr != nil {
			err = errors.Warning("sql: begin transaction failed").WithCause(acquireErr)
			return
		}
		withTransaction(r, tx)
		v = transactionAddress{
			Id:         unsafe.String(unsafe.SliceData(tid), len(tid)),
			EndpointId: fn.endpointId,
		}
		return
	}
	param, paramErr := services.ValueOfParam[transactionBeginParam](r.Param())
	if paramErr != nil {
		err = errors.Warning("sql: begin transaction failed").WithCause(paramErr)
		return
	}
	if param.Isolation == 0 {
		param.Isolation = fn.isolation
	}
	value, beginErr := fn.db.Begin(r, databases.TransactionOptions{
		Isolation: param.Isolation,
		Readonly:  param.Readonly,
	})
	if beginErr != nil {
		err = errors.Warning("sql: begin transaction failed").WithCause(beginErr)
		return
	}
	tx, has = fn.group.Set(tid, value)
	if !has {
		err = errors.Warning("sql: begin transaction failed").WithCause(fmt.Errorf("maybe duplicate begon"))
		return
	}
	withTransaction(r, tx)
	v = transactionAddress{
		Id:         unsafe.String(unsafe.SliceData(tid), len(tid)),
		EndpointId: fn.endpointId,
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	transactionCommitFnName = []byte("commit")
)

func Commit(ctx context.Context) (err error) {
	info, has, loadErr := loadTransactionInfo(ctx)
	if loadErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(loadErr)
		return
	}
	if !has {
		err = errors.Warning("sql: commit transaction failed").WithCause(fmt.Errorf("transaction maybe not begin"))
		return
	}
	r, ok := services.TryLoadRequest(ctx)
	if !ok {
		err = errors.Warning("sql: commit transaction failed").WithCause(fmt.Errorf("context is not endpoint request"))
		return
	}
	if info.Origin != bytex.ToString(r.Header().ProcessId()) {
		return
	}
	eps := runtime.Endpoints(ctx)
	ep := endpointName
	if epn := loadEndpointName(ctx); len(epn) > 0 {
		ep = epn
	}
	response, handleErr := eps.Request(ctx, ep, transactionCommitFnName, nil)
	if handleErr != nil {
		err = handleErr
		return
	}
	committed, responseErr := services.ValueOfResponse[bool](response)
	if responseErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(responseErr)
		return
	}
	log := logs.Load(ctx)
	if log.DebugEnabled() {
		if committed {
			log.Debug().With("transaction", "commit").Caller().Message(fmt.Sprintf("sql: transaction committed"))
		} else {
			log.Debug().With("transaction", "commit").Caller().Message(fmt.Sprintf("sql: transaction holdon"))
		}
	}
	return
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
	info, has, loadErr := loadTransactionInfo(r)
	if loadErr != nil {
		err = errors.Warning("sql: commit transaction failed").WithCause(loadErr)
		return
	}
	if !has {
		err = errors.Warning("sql: commit transaction failed").WithCause(fmt.Errorf("transaction maybe not begin"))
		return
	}
	if info.EndpointId != fn.endpointId {
		v = false
		return
	}
	if info.Origin != bytex.ToString(r.Header().ProcessId()) {
		v = false
		return
	}
	tid := bytex.FromString(info.Id)
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
	v = false
	if tx.Closed() {
		v = true
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
	// try load local
	tx, hasTx, txErr := loadTransaction(ctx)
	if txErr != nil {
		if log.DebugEnabled() {
			log.Debug().With("transaction", "rollback").Cause(txErr).Caller().Message(fmt.Sprintf("sql: transaction rollback failed"))
		}
		return
	}
	if hasTx {
		if tx.Closed() {
			// has rollback or committed
			return
		}
		return
	}
	// load info
	info, has, loadErr := loadTransactionInfo(ctx)
	if loadErr != nil {
		if log.DebugEnabled() {
			log.Debug().With("transaction", "rollback").Cause(loadErr).Caller().Message(fmt.Sprintf("sql: transaction rollback failed"))
		}
		return
	}
	if !has {
		if log.DebugEnabled() {
			log.Debug().With("transaction", "rollback").Cause(errors.Warning("sql: no transaction")).Caller().Message(fmt.Sprintf("sql: transaction rollback failed"))
		}
		return
	}
	eps := runtime.Endpoints(ctx)
	ep := endpointName
	if epn := loadEndpointName(ctx); len(epn) > 0 {
		ep = epn
	}
	_, handleErr := eps.Request(ctx, ep, transactionRollbackFnName, nil, services.WithEndpointId(bytex.FromString(info.EndpointId)))
	if handleErr != nil {
		if log.DebugEnabled() {
			log.Debug().With("transaction", "rollback").Cause(handleErr).Caller().Message(fmt.Sprintf("sql: transaction rollback failed"))
		}
		return
	}
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
	info, has, loadErr := loadTransactionInfo(r)
	if loadErr != nil {
		err = errors.Warning("sql: rollback transaction failed").WithCause(loadErr)
		return
	}
	if !has {
		err = errors.Warning("sql: rollback transaction failed").WithCause(fmt.Errorf("transaction maybe not begin"))
		return
	}
	if info.EndpointId != fn.endpointId {
		err = errors.Warning("sql: rollback transaction failed").WithCause(fmt.Errorf("transaction was not holdon in this endpoint"))
		return
	}
	tid := bytex.FromString(info.Id)
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
