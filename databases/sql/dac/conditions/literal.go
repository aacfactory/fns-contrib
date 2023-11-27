package conditions

import (
	"github.com/aacfactory/fns/commons/times"
	"strconv"
	"time"
	"unsafe"
)

func String(s string) Literal {
	return Literal{
		value: "'" + s + "'",
	}
}

func Bool(b bool) Literal {
	return Literal{
		value: strconv.FormatBool(b),
	}
}

func Int(n int) Literal {
	return Literal{
		value: strconv.Itoa(n),
	}
}

func Int64(n int64) Literal {
	return Literal{
		value: strconv.FormatInt(n, 10),
	}
}

func Float(f float32) Literal {
	return Literal{
		value: strconv.FormatFloat(float64(f), 'f', -1, 32),
	}
}

func Float64(f float64) Literal {
	return Literal{
		value: strconv.FormatFloat(f, 'f', -1, 64),
	}
}

func Uint(n uint) Literal {
	return Literal{
		value: strconv.FormatUint(uint64(n), 10),
	}
}

func Uint64(n uint64) Literal {
	return Literal{
		value: strconv.FormatUint(n, 10),
	}
}

func Datetime(t time.Time) Literal {
	return Literal{
		value: "'" + t.Format(time.RFC3339Nano) + "'",
	}
}

func Date(t times.Date) Literal {
	return Literal{
		value: "'" + t.String() + "'",
	}
}

func Time(t times.Time) Literal {
	return Literal{
		value: "'" + t.String() + "'",
	}
}

func Null() Literal {
	return Literal{
		value: "null",
	}
}

func Lit(v string) Literal {
	return Literal{
		value: v,
	}
}

type Literal struct {
	value string
}

func (lit Literal) Value() string {
	return lit.value
}

func (lit Literal) Bytes() []byte {
	return unsafe.Slice(unsafe.StringData(lit.value), len(lit.value))
}
