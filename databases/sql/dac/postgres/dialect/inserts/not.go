package inserts

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewInsertWhenNotExistsGeneric(spec *specifications.Specification) (generic *InsertWhenNotExistsGeneric, err error) {

	return
}

type InsertWhenNotExistsGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *InsertWhenNotExistsGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table, src specifications.QueryExpr) (method specifications.Method, arguments []any, err error) {

	return
}
