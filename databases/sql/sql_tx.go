package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"time"
)

func DefaultTxOption() (v TxBeginParam) {
	v = TxBeginParam{
		Timeout:   2 * time.Second,
		Isolation: 0,
	}
	return
}

func TxOption(timeout string, isolation db.IsolationLevel) (v TxBeginParam) {
	d, parseErr := time.ParseDuration(timeout)
	if parseErr != nil {
		panic(fmt.Sprintf("parse sql tx timeout(%s) failed, %v", timeout, parseErr))
	}
	v = TxBeginParam{
		Timeout:   d,
		Isolation: isolation,
	}
	return
}

type TxBeginParam struct {
	Timeout   time.Duration     `json:"timeout,omitempty"`
	Isolation db.IsolationLevel `json:"isolation,omitempty"`
}

func (svc *service) getTx(ctx fns.Context) (tx *db.Tx, has bool) {
	tx, has = svc.gtm.GetTx(ctx)
	return
}

func (svc *service) txBegin(ctx fns.Context, param TxBeginParam) (err errors.CodeError) {
	txErr := svc.gtm.Begin(ctx, svc.client.Writer(), param.Isolation, param.Timeout)
	if txErr != nil {
		err = errors.ServiceError("fns SQL: begin tx failed").WithCause(txErr)
		return
	}
	return
}

func (svc *service) txCommit(ctx fns.Context) (err errors.CodeError) {
	commitErr := svc.gtm.Commit(ctx)
	if commitErr != nil {
		err = errors.ServiceError("fns SQL: commit tx failed").WithCause(commitErr)
		return
	}
	return
}

func (svc *service) txRollback(ctx fns.Context) (err errors.CodeError) {
	svc.gtm.Rollback(ctx)
	return
}
