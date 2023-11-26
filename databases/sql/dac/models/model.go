package models

type Info struct {
	Schema    string
	Name      string
	Conflicts []string
}

type Model interface {
	TableInfo() Info
}
