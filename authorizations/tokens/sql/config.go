package sql

type Config struct {
	Database string `json:"database" yaml:"database"`
	Schema   string `json:"schema" yaml:"schema"`
	Table    string `json:"table" yaml:"table"`
}
