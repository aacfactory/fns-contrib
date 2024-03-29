package inserts

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/bytex"
	"io"
)

func NewValueRender() ValueRender {
	return ValueRender{
		verIdx: -1,
		size:   0,
	}
}

type ValueRender struct {
	verIdx int
	size   int
}

func (value *ValueRender) Add() {
	value.size++
}

func (value *ValueRender) MarkAsVersion() {
	value.verIdx = value.size - 1
}

func (value *ValueRender) Render(ctx specifications.Context, w io.Writer) (err error) {
	_, _ = w.Write(specifications.LB)
	for i := 0; i < value.size; i++ {
		if i > 0 {
			_, _ = w.Write(specifications.COMMA)
		}
		if i == value.verIdx {
			_, _ = w.Write([]byte("1"))
			continue
		}
		_, _ = w.Write(bytex.FromString(ctx.NextQueryPlaceholder()))
	}
	_, _ = w.Write(specifications.RB)
	return
}
