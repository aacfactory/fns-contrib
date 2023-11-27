package inserts

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewInsertGeneric(spec *specifications.Specification) (generic *InsertGeneric, err error) {

	return
}

type InsertGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *InsertGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table) (method specifications.Method, arguments []any, err error) {

	return
}
