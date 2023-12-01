package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
	"io"
)

var (
	SELECT    = []byte("SELECT")
	FORM      = []byte("FROM")
	WHERE     = []byte("WHERE")
	INSERT    = []byte("INSERT")
	UPDATE    = []byte("UPDATE")
	DELETE    = []byte("DELETE")
	SPACE     = []byte(" ")
	INTO      = []byte("INTO")
	AND       = []byte("AND")
	SET       = []byte("SET")
	EQ        = []byte("=")
	PLUS      = []byte("+")
	AT        = []byte("@")
	LB        = []byte("(")
	RB        = []byte(")")
	COMMA     = []byte(", ")
	DOT       = []byte(".")
	STAR      = []byte("*")
	AS        = []byte("AS")
	VALUES    = []byte("VALUES")
	EXISTS    = []byte("EXISTS")
	COUNT     = []byte("COUNT")
	NOT       = []byte("NOT")
	CONFLICT  = []byte("CONFLICT")
	ON        = []byte("ON")
	RETURNING = []byte("RETURNING")
	DO        = []byte("DO")
	NOTHING   = []byte("NOTHING")
	ORDER     = []byte("ORDER")
	BY        = []byte("BY")
	DESC      = []byte("DESC")
	GROUP     = []byte("GROUP")
	HAVING    = []byte("HAVING")
	OFFSET    = []byte("OFFSET")
	LIMIT     = []byte("LIMIT")
)

const (
	QueryMethod Method = iota + 1
	ExecuteMethod
)

type Method int

type QueryPlaceholder interface {
	Next() (v []byte)
	SkipCursor(n int)
}

type Render interface {
	Render(ctx Context, w io.Writer) (argument []any, err error)
}

type Dialect interface {
	Name() string
	FormatIdent(ident []byte) []byte
	QueryPlaceholder() QueryPlaceholder
	Insert(ctx Context, spec *Specification, values int) (method Method, query []byte, fields []int, returning []int, err error)
	InsertOrUpdate(ctx Context, spec *Specification) (method Method, query []byte, fields []int, returning []int, err error)
	InsertWhenExist(ctx Context, spec *Specification, src QueryExpr) (method Method, query []byte, fields []int, arguments []any, returning []int, err error)
	InsertWhenNotExist(ctx Context, spec *Specification, src QueryExpr) (method Method, query []byte, fields []int, arguments []any, returning []int, err error)
	Update(ctx Context, spec *Specification) (method Method, query []byte, fields []int, err error)
	UpdateFields(ctx Context, spec *Specification, fields []FieldValue, cond Condition) (method Method, query []byte, arguments []any, err error)
	Delete(ctx Context, spec *Specification) (method Method, query []byte, fields []int, err error)
	DeleteByConditions(ctx Context, spec *Specification, cond Condition) (method Method, query []byte, audits []int, arguments []any, err error)
	Exist(ctx Context, spec *Specification, cond Condition) (method Method, query []byte, arguments []any, err error)
	Count(ctx Context, spec *Specification, cond Condition) (method Method, query []byte, arguments []any, err error)
	Query(ctx Context, spec *Specification, cond Condition, orders Orders, groupBy GroupBy, having Having, offset int, length int) (method Method, query []byte, arguments []any, columns []int, err error)
}

var (
	dialects = make([]Dialect, 0, 1)
)

func RegisterDialect(dialect Dialect) {
	if dialect == nil {
		return
	}
	name := dialect.Name()
	if _, has := getDialect(name); has {
		panic(fmt.Errorf("%+v", errors.Warning(fmt.Sprintf("sql: %s dialect has registered", name))))
		return
	}
	dialects = append(dialects, dialect)
}

func getDialect(name string) (dialect Dialect, has bool) {
	for _, d := range dialects {
		if d.Name() == name {
			dialect = d
			has = true
			return
		}
	}
	return
}

func LoadDialect(ctx context.Context) (dialect Dialect, err error) {
	name, nameErr := sql.Dialect(ctx)
	if nameErr != nil {
		err = errors.Warning("sql: load dialect failed").WithCause(nameErr)
		return
	}
	has := name != ""
	if has {
		dialect, has = getDialect(name)
		if !has {
			err = errors.Warning("sql: load dialect failed").WithCause(fmt.Errorf("%s was not found", name))
			return
		}
		return
	}
	dialect, has = defaultDialect()
	if !has {
		err = errors.Warning("sql: load dialect failed").WithCause(fmt.Errorf("no dialect was registerd"))
		return
	}
	return
}

func defaultDialect() (dialect Dialect, has bool) {
	if len(dialects) == 0 {
		return
	}
	dialect = dialects[0]
	has = true
	return
}
