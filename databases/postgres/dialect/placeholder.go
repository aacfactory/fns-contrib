package dialect

import (
	"fmt"
)

type Placeholder struct {
	count int
}

func (ph *Placeholder) Next() (v string) {
	ph.count++
	v = fmt.Sprintf("$%d", ph.count)
	return v
}

func (ph *Placeholder) SkipCursor(n int) {
	ph.count = ph.count + n
}

func (ph *Placeholder) Current() (v string) {
	return fmt.Sprintf("$%d", ph.count)
}
