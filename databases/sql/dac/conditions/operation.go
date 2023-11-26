package conditions

import "unsafe"

const (
	AND = Operation("AND")
	OR  = Operation("OR")
)

type Operation string

func (op Operation) Bytes() []byte {
	s := string(op)
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
