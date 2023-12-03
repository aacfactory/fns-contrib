package dac

import (
	"github.com/aacfactory/fns/context"
	"strings"
)

type SequenceInfo struct {
	name    string
	schema  string
	options []string
}

func (info SequenceInfo) Name() string {
	return info.name
}

func (info SequenceInfo) Schema() string {
	return info.schema
}

func (info SequenceInfo) Options() []string {
	return info.options
}

type SequenceInfoOptions struct {
	schema  string
	options []string
}

type SequenceInfoOption func(options *SequenceInfoOptions)

func SequenceSchema(schema string) SequenceInfoOption {
	return func(options *SequenceInfoOptions) {
		options.schema = strings.TrimSpace(schema)
	}
}

func SequenceOptions(options ...string) SequenceInfoOption {
	return func(opt *SequenceInfoOptions) {
		opt.options = append(opt.options, options...)
	}
}

type Sequence interface {
	SequenceInfo() SequenceInfo
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
