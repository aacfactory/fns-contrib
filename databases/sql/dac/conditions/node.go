package conditions

import (
	"context"
	"io"
)

type RenderContext interface {
	context.Context
	AcquireQueryPlaceholder() (v []byte)
	// Localization
	// key maybe struct field or struct value
	// when field then return column name
	// when value then return table name
	Localization(key any) (content []byte, has bool)
}

type Node interface {
	Render(ctx RenderContext, w io.Writer) (argument []any, err error)
}
