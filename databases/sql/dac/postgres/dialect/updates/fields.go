package updates

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

func NewUpdateFieldsGeneric(spec *specifications.Specification) (generic *UpdateFieldsGeneric, err error) {

	return
}

type UpdateFieldsGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *UpdateFieldsGeneric) Render(ctx specifications.Context, w io.Writer, fields []specifications.FieldValue, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {

	return
}
