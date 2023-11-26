package conditions

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/languages"
	"github.com/valyala/bytebufferpool"
	"io"
	"reflect"
)

func New(predicate Predicate) Condition {
	return Condition{
		Operation: "",
		Left:      predicate,
		Right:     nil,
		Group:     false,
	}
}

// Condition
// tree
type Condition struct {
	Operation Operation
	Left      Node
	Right     Node
	Group     bool
}

func (cond Condition) Exist() bool {
	return cond.Left != nil
}

func (cond Condition) join(op Operation, right Node) (n Condition) {
	n.Operation = op
	if cond.Operation == "" {
		n.Left = cond.Left
	} else {
		n.Left = cond
	}
	r, ok := right.(Condition)
	if ok {
		r.Group = true
		n.Right = r
	} else {
		n.Right = right
	}
	return
}
func (cond Condition) And(right Node) (n Condition) {
	return cond.join(AND, right)
}

func (cond Condition) Or(right Node) (n Condition) {
	return cond.join(OR, right)
}

func (cond Condition) Render(ctx RenderContext, w io.Writer) (arguments []any, err error) {
	if cond.Left == nil {
		return
	}
	switch left := cond.Left.(type) {
	case Predicate:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		args, rErr := left.Render(ctx, buf)
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
	case Condition:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		if left.Group {
			_, _ = buf.Write(languages.LB)
		}
		args, rErr := left.Render(ctx, buf)
		if rErr != nil {
			err = rErr
			return
		}
		if left.Group {
			_, _ = buf.Write(languages.RB)
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
	_, _ = w.Write(languages.SPACE)
	_, _ = w.Write(cond.Operation.Bytes())
	_, _ = w.Write(languages.SPACE)
	switch right := cond.Right.(type) {
	case Predicate:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		args, rErr := right.Render(ctx, buf)
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
	case Condition:
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		if right.Group {
			_, _ = buf.Write(languages.LB)
		}
		args, rErr := right.Render(ctx, buf)
		if rErr != nil {
			err = rErr
			return
		}
		if right.Group {
			_, _ = buf.Write(languages.RB)
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
