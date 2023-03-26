package sql

type Config struct {
	Database  string `json:"database"`
	RoleTable Table  `json:"roleTable"`
	UserTable Table  `json:"userTable"`
}

type Table struct {
	Schema string `json:"schema" yaml:"schema"`
	Table  string `json:"table" yaml:"table"`
}
