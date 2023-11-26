package models

type TableInfo struct {
	Schema    string
	Name      string
	Conflicts []string
}

func (info TableInfo) In(schema string) TableInfo {
	info.Schema = schema
	return info
}

func (info TableInfo) AddConflicts(conflicts ...string) TableInfo {
	info.Conflicts = append(info.Conflicts, conflicts...)
	return info
}

func Info(name string) TableInfo {
	return TableInfo{
		Schema:    "",
		Name:      name,
		Conflicts: nil,
	}
}

type Table interface {
	TableInfo() TableInfo
}
