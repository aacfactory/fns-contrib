package models

import (
	"context"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/selects"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/updates"
	"io"
)

var (
	SELECT = []byte("SELECT")
	FORM   = []byte("FROM")
	WHERE  = []byte("WHERE")
	INSERT = []byte("INSERT")
	UPDATE = []byte("UPDATE")
	DELETE = []byte("DELETE")
	SPACE  = []byte(" ")
	AT     = []byte("@")
	LB     = []byte("(")
	RB     = []byte(")")
	COMMA  = []byte(", ")
)

type Method string

type QueryPlaceholder interface {
	Next() (v []byte)
}

type Render interface {
	Render(ctx Context, w io.Writer) (argument []any, err error)
}

type Dialect interface {
	FormatIdent(ident string) string
	QueryPlaceholder() QueryPlaceholder
	Insert(ctx context.Context, table Table) (method Method, query []byte, arguments []any, err error)
	InsertOrUpdate(ctx context.Context, table Table) (method Method, query []byte, arguments []any, err error)
	InsertWhenExist(ctx context.Context, table Table, source string) (method Method, query []byte, arguments []any, err error)
	InsertWhenNotExist(ctx context.Context, table Table, source string) (method Method, query []byte, arguments []any, err error)
	Update(ctx context.Context, table Table) (method Method, query []byte, arguments []any, err error)
	UpdateFields(ctx context.Context, fields updates.Fields, cond conditions.Condition) (method Method, query []byte, arguments []any, err error)
	Delete(ctx context.Context, table Table) (method Method, query []byte, arguments []any, err error)
	DeleteWithConditions(ctx context.Context, cond conditions.Condition) (method Method, query []byte, arguments []any, err error)
	Exist(ctx context.Context, cond conditions.Condition) (method Method, query []byte, arguments []any, err error)
	Count(ctx context.Context, cond conditions.Condition) (method Method, query []byte, arguments []any, err error)
	Select(ctx context.Context, cond conditions.Condition, orders selects.Orders, rng selects.Range) (method Method, query []byte, arguments []any, err error)
}
