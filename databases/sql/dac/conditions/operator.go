package conditions

import "unsafe"

const (
	Equal            = Operator("=")
	NotEqual         = Operator("<>")
	GreatThan        = Operator(">")
	GreatThanOrEqual = Operator(">=")
	LessThan         = Operator("<")
	LessThanOrEqual  = Operator("<=")
	BETWEEN          = Operator("BETWEEN")
	IN               = Operator("IN")
	NOTIN            = Operator("NOT IN")
	LIKE             = Operator("LIKE")
)

type Operator string

func (op Operator) Bytes() []byte {
	s := string(op)
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
