package specifications_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"testing"
)

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

type SomeTable struct {
}

func (t SomeTable) TableInfo() TableInfo {
	return TableInfo{
		name:      "name",
		schema:    "schema",
		view:      true,
		conflicts: []string{"f1", "f2"},
	}
}

func TestGetTableInfo(t *testing.T) {
	info, infoErr := specifications.GetTableInfo(SomeTable{})
	if infoErr != nil {
		fmt.Println(fmt.Sprintf("%+v", infoErr))
		return
	}
	fmt.Println(fmt.Sprintf("%+v", info))
}
