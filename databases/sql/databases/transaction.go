package databases

import (
	"context"
	"database/sql"
	"github.com/aacfactory/errors"
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
	core       *sql.Tx
	prepare    bool
	statements *Statements
}

func (tx *DefaultTransaction) Commit() error {
	return tx.core.Commit()
}

func (tx *DefaultTransaction) Rollback() error {
	return tx.core.Rollback()
}

func (tx *DefaultTransaction) Query(ctx context.Context, query []byte, args []interface{}) (rows Rows, err error) {
	var r *sql.Rows
	if tx.prepare {
		stmt, prepareErr := tx.statements.Get(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		r, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			if errors.Contains(err, ErrStatementClosed) {
				rows, err = tx.Query(ctx, query, args)
				return
			}
			return
		}
	} else {
		r, err = tx.core.QueryContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
		if err != nil {
			return
		}
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (tx *DefaultTransaction) Execute(ctx context.Context, query []byte, args []interface{}) (result Result, err error) {
	var r sql.Result
	if tx.prepare {
		stmt, prepareErr := tx.statements.Get(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		r, err = stmt.ExecContext(ctx, args...)
		if err != nil {
			if errors.Contains(err, ErrStatementClosed) {
				result, err = tx.Execute(ctx, query, args)
				return
			}
			return
		}
	} else {
		r, err = tx.core.ExecContext(ctx, unsafe.String(unsafe.SliceData(query), len(query)), args...)
		if err != nil {
			return
		}
	}
	rowsAffected, rowsAffectedErr := r.RowsAffected()
	if rowsAffectedErr != nil {
		err = rowsAffectedErr
		return
	}
	lastInsertId, lastInsertIdErr := r.LastInsertId()
	if lastInsertIdErr != nil {
		err = lastInsertIdErr
		return
	}
	result = Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}
	return
}
