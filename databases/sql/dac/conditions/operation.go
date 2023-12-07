package conditions

const (
	AND = Operation("AND")
	OR  = Operation("OR")
)

type Operation string

func (op Operation) String() string {
	return string(op)
}
