package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"time"
)

func DefaultTransactionOption() (v BeginTransactionParam) {
	v = BeginTransactionParam{
		Timeout:   2 * time.Second,
		Isolation: 0,
	}
	return
}

func TxOption(timeout string, isolation db.IsolationLevel) (v BeginTransactionParam) {
	d, parseErr := time.ParseDuration(timeout)
	if parseErr != nil {
		panic(fmt.Sprintf("parse sql tx timeout(%s) failed, %v", timeout, parseErr))
	}
	v = BeginTransactionParam{
		Timeout:   d,
		Isolation: isolation,
	}
	return
}

type BeginTransactionParam struct {
	Timeout   time.Duration     `json:"timeout,omitempty"`
	Isolation db.IsolationLevel `json:"isolation,omitempty"`
}

func (svc *service) getTransaction(ctx fns.Context) (tx *db.Tx, has bool) {
	tx, has = svc.gtm.Get(ctx)
	return
}

func (svc *service) beginTransaction(ctx fns.Context, param BeginTransactionParam) (err errors.CodeError) {
	txErr := svc.gtm.Begin(ctx, svc.client.Writer(), param.Isolation, param.Timeout)
	if txErr != nil {
		err = errors.ServiceError("fns SQL: begin tx failed").WithCause(txErr)
		return
	}
	return
}

func (svc *service) commitTransaction(ctx fns.Context) (err errors.CodeError) {
	commitErr := svc.gtm.Commit(ctx)
	if commitErr != nil {
		err = errors.ServiceError("fns SQL: commit tx failed").WithCause(commitErr)
		return
	}
	return
}

func (svc *service) rollbackTransaction(ctx fns.Context) (err errors.CodeError) {
	svc.gtm.Rollback(ctx)
	return
}
