package sql

import (
	db "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"time"
)

func (svc *Service) getTx(ctx fns.Context) (tx *db.Tx, has bool) {
	tx, has = svc.gtm.Get(ctx)
	return
}

type TxBeginParam struct {
	Timeout   time.Duration     `json:"timeout,omitempty"`
	Isolation db.IsolationLevel `json:"isolation,omitempty"`
}

func (svc *Service) txBegin(ctx fns.Context, param TxBeginParam) (err errors.CodeError) {
	_, has := svc.gtm.Get(ctx)
	if has {
		return
	}

	tx, txErr := svc.client.Writer().BeginTx(ctx, &db.TxOptions{
		Isolation: param.Isolation,
		ReadOnly:  false,
	})

	if txErr != nil {
		err = errors.ServiceError("fns SQL: begin tx failed").WithCause(txErr)
		return
	}

	setErr := svc.gtm.Set(ctx, tx, param.Timeout)
	if setErr != nil {
		_ = tx.Rollback()
	}

	return
}

func (svc *Service) txCommit(ctx fns.Context) (err errors.CodeError) {
	tx, has := svc.gtm.Get(ctx)
	if !has {
		err = errors.ServiceError("fns SQL: commit tx failed for tx was not found")
		return
	}

	commitErr := tx.Commit()
	if commitErr != nil {
		_ = tx.Rollback()
		svc.gtm.Del(ctx)
		err = errors.ServiceError("fns SQL: commit tx failed").WithCause(commitErr)
		return
	}

	svc.gtm.Del(ctx)

	return
}

func (svc *Service) txRollback(ctx fns.Context) (err errors.CodeError) {
	tx, has := svc.gtm.Get(ctx)
	if !has {
		err = errors.ServiceError("fns SQL: rollback tx failed for tx was not found")
		return
	}

	_ = tx.Rollback()
	svc.gtm.Del(ctx)

	return
}
