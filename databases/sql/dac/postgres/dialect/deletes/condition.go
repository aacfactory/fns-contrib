package deletes

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewDeleteByConditionsGeneric(spec *specifications.Specification) (generic *DeleteByConditionsGeneric, err error) {

	return
}

type DeleteByConditionsGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *DeleteByConditionsGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {

	return
}
