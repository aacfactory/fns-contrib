package generators

import (
	"context"
	"github.com/aacfactory/gcg"
)

const (
	begin = "begin"
)

// TransactionWriter
// @postgres {endpoint} begin
type TransactionWriter struct {
}

func (writer *TransactionWriter) Annotation() (annotation string) {
	return "postgres"
}

func (writer *TransactionWriter) HandleBefore(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	return
}

func (writer *TransactionWriter) HandleAfter(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	return
}

func (writer *TransactionWriter) ProxyBefore(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {

	return
}

func (writer *TransactionWriter) ProxyAfter(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {

	return
}
