package deletes

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewDeleteGeneric(spec *specifications.Specification) (generic *DeleteGeneric, err error) {

	return
}

type DeleteGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *DeleteGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table) (method specifications.Method, arguments []any, err error) {

	return
}
