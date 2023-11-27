package postgres

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
