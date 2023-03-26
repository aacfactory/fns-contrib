package redis

type Config struct {
	Database  string `json:"database" yaml:"database"`
	KeyPrefix string `json:"keyPrefix" yaml:"keyPrefix"`
}
