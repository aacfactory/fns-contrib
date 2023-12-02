package dac

import "github.com/aacfactory/fns/context"

func SequenceNextValue(ctx context.Context, sequence string) (n int64, err error) {
	// SELECT nextval('#name#')
	return
}
