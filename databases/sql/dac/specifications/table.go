package specifications

func NewTableInfo(schema string, name string, conflicts []string, tree []string) TableInfo {
	return TableInfo{schema: schema, name: name, conflicts: conflicts, tree: tree}
}

type TableInfo struct {
	schema    string
	name      string
	conflicts []string
	tree      []string
}

type Table interface {
	TableInfo() TableInfo
}
