package dac

import (
	"strings"
)

type TableInfoOptions struct {
	schema    string
	conflicts []string
}

type TableInfoOption func(options *TableInfoOptions)

func Schema(schema string) TableInfoOption {
	return func(options *TableInfoOptions) {
		options.schema = strings.TrimSpace(schema)
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
		conflicts: opt.conflicts,
	}
}

type TableInfo struct {
	name      string
	schema    string
	conflicts []string
}

func (info TableInfo) Schema() string {
	return info.schema
}

func (info TableInfo) Name() string {
	return info.name
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
	pure   bool
	name   string
	schema string
	base   Table
}

func (info ViewInfo) Pure() (string, string, bool) {
	return info.schema, info.name, info.pure
}

func (info ViewInfo) Base() Table {
	return info.base
}

func TableView(table Table) ViewInfo {
	return ViewInfo{
		pure:   false,
		name:   "",
		schema: "",
		base:   table,
	}
}

func PureView(name string, schema ...string) ViewInfo {
	s := ""
	if len(schema) > 0 {
		s = schema[0]
	}
	return ViewInfo{
		pure:   true,
		name:   strings.TrimSpace(name),
		schema: s,
		base:   nil,
	}
}

type View interface {
	ViewInfo() ViewInfo
}
