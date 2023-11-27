package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/valyala/bytebufferpool"
	"io"
	"reflect"
)

type Condition struct {
	conditions.Condition
}

func (cond Condition) Render(ctx Context, w io.Writer) (arguments []any, err error) {
	if cond.Left == nil {
		return
	}
	switch left := cond.Left.(type) {
	case conditions.Predicate:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		args, rErr := Predicate{left}.Render(ctx, buf)
		if rErr != nil {
			err = rErr
			return
		}
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: condition render failed").WithCause(err)
			return
		}
		arguments = append(arguments, args...)
		break
	case conditions.Condition:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		if left.Group {
			_, _ = buf.Write(LB)
		}
		args, rErr := Condition{left}.Render(ctx, buf)
		if rErr != nil {
			err = rErr
			return
		}
		if left.Group {
			_, _ = buf.Write(RB)
		}
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: condition render failed").WithCause(err)
			return
		}
		arguments = append(arguments, args...)
		break
	default:
		err = errors.Warning("sql: condition render failed").WithCause(fmt.Errorf("%s is not supported", reflect.TypeOf(left).String()))
		return
	}
	if cond.Operation == "" {
		return
	}
	if cond.Right == nil {
		err = errors.Warning("sql: condition render failed").WithCause(fmt.Errorf("invalid condition"))
		return
	}
	_, _ = w.Write(SPACE)
	_, _ = w.Write(cond.Operation.Bytes())
	_, _ = w.Write(SPACE)
	switch right := cond.Right.(type) {
	case conditions.Predicate:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		args, rErr := Predicate{right}.Render(ctx, buf)
		if rErr != nil {
			err = rErr
			return
		}
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: condition render failed").WithCause(err)
			return
		}
		arguments = append(arguments, args...)
		break
	case conditions.Condition:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		if right.Group {
			_, _ = buf.Write(LB)
		}
		args, rErr := Condition{right}.Render(ctx, buf)
		if rErr != nil {
			err = rErr
			return
		}
		if right.Group {
			_, _ = buf.Write(RB)
		}
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: condition render failed").WithCause(err)
			return
		}
		arguments = append(arguments, args...)
		break
	default:
		err = errors.Warning("sql: condition render failed").WithCause(fmt.Errorf("%s is not supported", reflect.TypeOf(right).String()))
		return
	}
	return
}
