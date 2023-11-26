package models

type Info struct {
	Schema    string
	Name      string
	Conflicts []string
}

func (info Info) AddConflicts(conflicts ...string) Info {
	info.Conflicts = append(info.Conflicts, conflicts...)
	return info
}

func Table(schema string, name string) Info {
	return Info{
		Schema:    schema,
		Name:      name,
		Conflicts: nil,
	}
}

type Model interface {
	TableInfo() Info
}
