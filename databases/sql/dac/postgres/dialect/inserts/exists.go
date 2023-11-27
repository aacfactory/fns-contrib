package inserts

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewInsertWhenExistsGeneric(spec *specifications.Specification) (generic *InsertWhenExistsGeneric, err error) {

	return
}

type InsertWhenExistsGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *InsertWhenExistsGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table, src specifications.QueryExpr) (method specifications.Method, arguments []any, err error) {

	return
}
