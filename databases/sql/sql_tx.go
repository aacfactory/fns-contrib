package sql

import (
	db "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

func DefaultTransactionOption() (v BeginTransactionParam) {
	v = BeginTransactionParam{
		Isolation: 0,
		ReadOnly:  false,
	}
	return
}

func TransactionOption(isolation db.IsolationLevel, readOnly bool) (v BeginTransactionParam) {
	v = BeginTransactionParam{
		Isolation: isolation,
		ReadOnly:  readOnly,
	}
	return
}

type BeginTransactionParam struct {
	Isolation db.IsolationLevel `json:"isolation,omitempty"`
	ReadOnly  bool              `json:"readOnly"`
}

func (svc *service) getTransaction(ctx fns.Context) (tx *db.Tx, has bool) {
	tx, has = svc.gtm.Get(ctx)
	return
}

func (svc *service) beginTransaction(ctx fns.Context, param BeginTransactionParam) (err errors.CodeError) {
	txErr := svc.gtm.Begin(ctx, svc.client.Writer(), param.Isolation, param.ReadOnly)
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
