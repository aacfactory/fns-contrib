package conditions

import "io"

func New(predicate Predicate) Condition {
	return Condition{
		Operation: "",
		Left:      predicate,
		Right:     nil,
	}
}

// Condition
// tree
type Condition struct {
	Operation Operation
	Left      Node
	Right     Node
}

func (cond Condition) Exist() bool {
	return cond.Left != nil
}

func (cond Condition) And(next Condition) (n Condition) {
	n.Operation = AND
	n.Left = cond
	n.Right = next
	return
}

func (cond Condition) Or(next Condition) (n Condition) {
	n.Operation = OR
	n.Left = cond
	n.Right = next
	return
}

func (cond Condition) Render(ctx RenderContext, w io.Writer) (argument []any, err error) {

	return
}

func (cond Condition) Arguments() (v []interface{}) {
	head := cond.head()
	switch x := head.Left.(type) {
	case Condition:
		v = append(v, x.Arguments()...)
		break
	case Predicate:
		break
	}
	return
}

func (cond Condition) head() Condition {
	if cond.Operation == "" {
		return cond
	}
	return cond.Left.(Condition).head()
}
