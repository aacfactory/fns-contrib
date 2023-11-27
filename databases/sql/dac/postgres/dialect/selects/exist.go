package selects

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewExistGeneric(spec *specifications.Specification) (generic *ExistGeneric, err error) {

	return
}

type ExistGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *ExistGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {

	return
}
