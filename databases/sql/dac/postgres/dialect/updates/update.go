package updates

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewUpdateGeneric(spec *specifications.Specification) (generic *UpdateGeneric, err error) {

	return
}

type UpdateGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *UpdateGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table) (method specifications.Method, arguments []any, err error) {

	return
}
