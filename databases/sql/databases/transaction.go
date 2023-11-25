package databases

import (
	"context"
	"database/sql"
	"unsafe"
)

type TransactionOptions struct {
	Isolation Isolation
	Readonly  bool
}

type TransactionOption func(options *TransactionOptions)

type Transaction interface {
	Commit() error
	Rollback() error
	Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error)
	Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error)
}

type DefaultTransaction struct {
	core *sql.Tx
}

func (tx *DefaultTransaction) Commit() error {
	return tx.core.Commit()
}

func (tx *DefaultTransaction) Rollback() error {
	return tx.core.Rollback()
}

func (tx *DefaultTransaction) Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error) {
	r, queryErr := tx.core.QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
	if queryErr != nil {
		err = queryErr
		return
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (tx *DefaultTransaction) Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error) {
	r, execErr := tx.core.ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
	if execErr != nil {
		err = execErr
		return
	}
	lastInsertId, lastInsertIdErr := r.LastInsertId()
	if lastInsertIdErr != nil {
		err = lastInsertIdErr
		return
	}
	rowsAffected, rowsAffectedErr := r.RowsAffected()
	if rowsAffectedErr != nil {
		err = rowsAffectedErr
		return
	}
	result = Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}
	return
}
