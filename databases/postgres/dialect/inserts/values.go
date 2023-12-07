package inserts

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
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
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(specifications.LB)
	for i := 0; i < value.size; i++ {
		if i > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		if i == value.verIdx {
			_, _ = buf.Write([]byte("1"))
			continue
		}
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
	}
	_, _ = buf.Write(specifications.RB)
	p := buf.String()
	_, err = w.Write([]byte(p))
	return
}
