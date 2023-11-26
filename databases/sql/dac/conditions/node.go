package conditions

import (
	"io"
)

type Node interface {
	Render(ctx Context, w io.Writer) (argument []any, err error)
}
