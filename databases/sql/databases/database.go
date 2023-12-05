package databases

import (
	"context"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/logs"
)

type Result struct {
	LastInsertId int64 `json:"lastInsertId"`
	RowsAffected int64 `json:"rowsAffected"`
}

type Options struct {
	Log    logs.Logger
	Config configures.Config
}

type Database interface {
	Name() string
	Construct(options Options) (err error)
	Begin(ctx context.Context, options TransactionOptions) (tx Transaction, err error)
	Query(ctx context.Context, query []byte, args []any) (rows Rows, err error)
	Execute(ctx context.Context, query []byte, args []any) (result Result, err error)
	Close(ctx context.Context) (err error)
}
