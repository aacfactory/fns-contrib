package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"strings"
)

type TableInfoOptions struct {
	schema    string
	view      bool
	conflicts []string
}

type TableInfoOption func(options *TableInfoOptions)

func Schema(schema string) TableInfoOption {
	return func(options *TableInfoOptions) {
		options.schema = strings.TrimSpace(schema)
	}
}

func View() TableInfoOption {
	return func(options *TableInfoOptions) {
		options.view = true
	}
}

// Conflicts
// params are field not column
func Conflicts(conflicts ...string) TableInfoOption {
	return func(options *TableInfoOptions) {
		for _, conflict := range conflicts {
			conflict = strings.TrimSpace(conflict)
			if conflict == "" {
				continue
			}
			options.conflicts = append(options.conflicts, conflict)
		}
	}
}

func TableInfo(name string, options ...TableInfoOption) specifications.TableInfo {
	opt := TableInfoOptions{}
	for _, option := range options {
		option(&opt)
	}
	return specifications.NewTableInfo(opt.schema, strings.TrimSpace(name), opt.view, opt.conflicts)
}

// Table
// the recv of TableInfo method must be value, can not be ptr
type Table interface {
	specifications.Table
}
