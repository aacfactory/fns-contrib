package selects

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewCountGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *CountGeneric, err error) {

	return
}

type CountGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *CountGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {

	return
}
