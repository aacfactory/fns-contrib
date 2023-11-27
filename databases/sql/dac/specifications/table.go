package specifications

func NewTableInfo(schema string, name string, view bool, conflicts []string, tree []string) TableInfo {
	return TableInfo{schema: schema, name: name, view: view, conflicts: conflicts, tree: tree}
}

type TableInfo struct {
	schema    string
	name      string
	view      bool
	conflicts []string
	tree      []string
}

type Table interface {
	TableInfo() TableInfo
}
