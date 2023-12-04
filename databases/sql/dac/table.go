package dac

import (
	"strings"
)

type TableInfoOptions struct {
	schema    string
	view      bool
	viewBase  Table
	conflicts []string
}

type TableInfoOption func(options *TableInfoOptions)

func Schema(schema string) TableInfoOption {
	return func(options *TableInfoOptions) {
		options.schema = strings.TrimSpace(schema)
	}
}

func View1(base Table) TableInfoOption {
	return func(options *TableInfoOptions) {
		options.view = true
		options.viewBase = base
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

func Info(name string, options ...TableInfoOption) TableInfo {
	opt := TableInfoOptions{}
	for _, option := range options {
		option(&opt)
	}
	return TableInfo{
		name:      strings.TrimSpace(name),
		schema:    opt.schema,
		view:      opt.view,
		conflicts: opt.conflicts,
	}
}

type TableInfo struct {
	name      string
	schema    string
	view      bool
	conflicts []string
}

func (info TableInfo) Schema() string {
	return info.schema
}

func (info TableInfo) Name() string {
	return info.name
}

func (info TableInfo) View() bool {
	return info.view
}

func (info TableInfo) Conflicts() []string {
	return info.conflicts
}

// Table
// the recv of TableInfo method must be value, can not be ptr
type Table interface {
	TableInfo() TableInfo
}

type ViewInfo struct {
}

type View interface {
}
