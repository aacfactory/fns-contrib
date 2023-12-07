package databases

import (
	"context"
	"database/sql"
)

type TransactionOptions struct {
	Isolation Isolation
	Readonly  bool
}

type TransactionOption func(options *TransactionOptions)

type Transaction interface {
	Commit() error
	Rollback() error
	Query(ctx context.Context, query string, args []any) (rows Rows, err error)
	Execute(ctx context.Context, query string, args []any) (result Result, err error)
}

func NewTransactionWithStatements(tx *sql.Tx, statements *Statements) Transaction {
	return &DefaultTransaction{
		core:       tx,
		prepare:    statements != nil,
		statements: statements,
	}
}

func NewTransaction(tx *sql.Tx) Transaction {
	return &DefaultTransaction{
		core:       tx,
		prepare:    false,
		statements: nil,
	}
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

func (tx *DefaultTransaction) Query(ctx context.Context, query string, args []any) (rows Rows, err error) {
	var r *sql.Rows
	if tx.prepare {
		stmt, prepareErr := tx.statements.Get(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		st, release, closed := stmt.Stmt()
		if closed {
			rows, err = tx.Query(ctx, query, args)
			return
		}
		st = tx.core.Stmt(st)
		r, err = st.Query(args...)
		release()
		if err != nil {
			return
		}
	} else {
		r, err = tx.core.Query(query, args...)
		if err != nil {
			return
		}
	}
	rows = &DefaultRows{
		core: r,
	}
	return
}

func (tx *DefaultTransaction) Execute(ctx context.Context, query string, args []any) (result Result, err error) {
	var r sql.Result
	if tx.prepare {
		stmt, prepareErr := tx.statements.Get(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		st, release, closed := stmt.Stmt()
		if closed {
			result, err = tx.Execute(ctx, query, args)
			return
		}
		st = tx.core.Stmt(st)
		r, err = st.Exec(args...)
		release()
		if err != nil {
			return
		}
	} else {
		r, err = tx.core.Exec(query, args...)
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
		lastInsertId = -1
	}

	result = Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}
	return
}
