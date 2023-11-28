package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
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
	AS        = []byte("AS")
	VALUES    = []byte("VALUES")
	EXISTS    = []byte("EXISTS")
	NOT       = []byte("NOT")
	CONFLICT  = []byte("CONFLICT")
	ON        = []byte("ON")
	RETURNING = []byte("RETURNING")
	DO        = []byte("DO")
	NOTHING   = []byte("NOTHING")
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
	Insert(ctx Context, spec *Specification, instance Table) (method Method, query []byte, arguments []any, err error)
	InsertOrUpdate(ctx Context, spec *Specification, instance Table) (method Method, query []byte, arguments []any, err error)
	InsertWhenExist(ctx Context, spec *Specification, instance Table, src QueryExpr) (method Method, query []byte, arguments []any, err error)
	InsertWhenNotExist(ctx Context, spec *Specification, instance Table, src QueryExpr) (method Method, query []byte, arguments []any, err error)
	Update(ctx Context, spec *Specification, instance Table) (method Method, query []byte, arguments []any, err error)
	UpdateFields(ctx Context, spec *Specification, fields []FieldValue, cond Condition) (method Method, query []byte, arguments []any, err error)
	Delete(ctx Context, spec *Specification, instance Table) (method Method, query []byte, arguments []any, err error)
	DeleteByConditions(ctx Context, spec *Specification, cond Condition) (method Method, query []byte, arguments []any, err error)
	Exist(ctx Context, spec *Specification, cond Condition) (method Method, query []byte, arguments []any, err error)
	Count(ctx Context, spec *Specification, cond Condition) (method Method, query []byte, arguments []any, err error)
	Query(ctx Context, spec *Specification, cond Condition, orders Orders, groupBy GroupBy, having Having, offset int, length int) (method Method, query []byte, arguments []any, err error)
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

func defaultDialect() (dialect Dialect, has bool) {
	if len(dialects) == 0 {
		return
	}
	dialect = dialects[0]
	has = true
	return
}
