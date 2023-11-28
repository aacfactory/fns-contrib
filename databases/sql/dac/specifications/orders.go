package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/valyala/bytebufferpool"
	"io"
)

type Order struct {
	Name string
	Desc bool
}

type Orders []Order

func (o Orders) Asc(name string) Orders {
	return append(o, Order{Name: name, Desc: false})
}

func (o Orders) Desc(name string) Orders {
	return append(o, Order{Name: name, Desc: true})
}

func (o Orders) Render(ctx Context, w io.Writer) (argument []any, err error) {
	if len(o) == 0 {
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(ORDER)
	_, _ = buf.Write(SPACE)
	_, _ = buf.Write(BY)
	_, _ = buf.Write(SPACE)
	for i, order := range o {
		if i > 0 {
			_, _ = buf.Write(COMMA)
		}
		content, has := ctx.Localization(order.Name)
		if !has {
			err = errors.Warning("sql: render order by failed").WithCause(fmt.Errorf("%s was not found", order.Name))
			return
		}
		_, _ = buf.Write(content[0])
		if order.Desc {
			_, _ = buf.Write(SPACE)
			_, _ = buf.Write(DESC)
		}
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		err = errors.Warning("sql: render order by failed").WithCause(err)
		return
	}
	return
}

func Asc(name string) Orders {
	return Orders{{
		Name: name,
		Desc: false,
	}}
}

func Desc(name string) Orders {
	return Orders{{
		Name: name,
		Desc: true,
	}}
}
