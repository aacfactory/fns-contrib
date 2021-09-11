package sql

import (
	db "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"time"
)

type TxBeginParam struct {
	Timeout   time.Duration     `json:"timeout,omitempty"`
	Isolation db.IsolationLevel `json:"isolation,omitempty"`
}

func (svc *_service) getTx(ctx fns.Context) (tx *db.Tx, has bool) {
	tx, has = svc.gtm.GetTx(ctx)
	return
}

func (svc *_service) txBegin(ctx fns.Context, param TxBeginParam) (err errors.CodeError) {
	txErr := svc.gtm.Begin(ctx, svc.client.Writer(), param.Isolation, param.Timeout)
	if txErr != nil {
		err = errors.ServiceError("fns SQL: begin tx failed").WithCause(txErr)
		return
	}
	return
}

func (svc *_service) txCommit(ctx fns.Context) (err errors.CodeError) {
	commitErr := svc.gtm.Commit(ctx)
	if commitErr != nil {
		err = errors.ServiceError("fns SQL: commit tx failed").WithCause(commitErr)
		return
	}
	return
}

func (svc *_service) txRollback(ctx fns.Context) (err errors.CodeError) {
	svc.gtm.Rollback(ctx)
	return
}
