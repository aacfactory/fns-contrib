package dialect

import (
	"strconv"
)

type Placeholder struct {
	count int
}

func (ph *Placeholder) Next() (v []byte) {
	ph.count++
	v = append(v, '$')
	v = append(v, strconv.Itoa(ph.count)...)
	return v
}

func (ph *Placeholder) SkipCursor(n int) {
	ph.count = ph.count + n
}
