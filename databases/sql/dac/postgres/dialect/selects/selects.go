package selects

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewQueryGeneric(spec *specifications.Specification) (generic *QueryGeneric, err error) {

	return
}

type QueryGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *QueryGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition, orders specifications.Orders, groupBy specifications.GroupBy, having specifications.Having, offset int, length int) (method specifications.Method, arguments []any, err error) {

	return
}
