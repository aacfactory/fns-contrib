package inserts

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewInsertOrUpdateGeneric(spec *specifications.Specification) (generic *InsertOrUpdateGeneric, err error) {

	return
}

type InsertOrUpdateGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *InsertOrUpdateGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table) (method specifications.Method, arguments []any, err error) {

	return
}
