package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

type Sequence interface {
	specifications.Sequence
}

func SequenceNextValue[S Sequence](ctx context.Context) (n int64, err error) {
	// SELECT nextval('#name#')
	return
}

func SequenceCurrentValue[S Sequence](ctx context.Context) (n int64, err error) {
	// pg: SELECT currval('#name#')
	// when seq was not nextval again, then will return err
	return
}
