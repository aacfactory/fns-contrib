package dac

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"strings"
)

type TableInfoOptions struct {
	schema    string
	view      bool
	conflicts []string
	tree      []string
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

func TreeFields(parent string, children string) TableInfoOption {
	return func(options *TableInfoOptions) {
		parent = strings.TrimSpace(parent)
		if parent == "" {
			panic(fmt.Errorf("%+v", errors.Warning("sql: set table tree fields failed").WithCause(fmt.Errorf("parent field name is nil"))))
		}
		children = strings.TrimSpace(children)
		if children == "" {
			panic(fmt.Errorf("%+v", errors.Warning("sql: set table tree fields failed").WithCause(fmt.Errorf("children field name is nil"))))
		}
		options.tree = []string{parent, children}
	}
}

func TableInfo(name string, options ...TableInfoOption) specifications.TableInfo {
	opt := TableInfoOptions{}
	for _, option := range options {
		option(&opt)
	}
	return specifications.NewTableInfo(opt.schema, strings.TrimSpace(name), opt.view, opt.conflicts, opt.tree)
}

// Table
// the recv of TableInfo method must be value, can not be ptr
type Table interface {
	specifications.Table
}
