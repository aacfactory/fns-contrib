package conditions

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
	if ok && r.Operation != "" {
		r.Group = true
		n.Right = r
	} else {
		n.Right = right
	}
	return
}

func (cond Condition) name() string {
	return "condition"
}

func (cond Condition) And(right Node) (n Condition) {
	return cond.join(AND, right)
}

func (cond Condition) Or(right Node) (n Condition) {
	return cond.join(OR, right)
}
