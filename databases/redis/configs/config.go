package configs

import "github.com/redis/rueidis"

type Config struct {
	Addr            []string   `json:"addr"`
	Username        string     `json:"username"`
	Password        string     `json:"password"`
	ClientName      string     `json:"clientName"`
	DB              int        `json:"db"`
	PoolSize        int        `json:"poolSize"`
	PoolTimeout     string     `json:"poolTimeout"`
	MaxRetries      int        `json:"maxRetries"`
	MinRetryBackoff string     `json:"minRetryBackoff"`
	MaxRetryBackoff string     `json:"maxRetryBackoff"`
	DialTimeout     string     `json:"dialTimeout"`
	ReadTimeout     string     `json:"readTimeout"`
	WriteTimeout    string     `json:"writeTimeout"`
	MinIdleConns    int        `json:"minIdleConns"`
	MaxIdleConns    int        `json:"maxIdleConns"`
	ConnMaxIdleTime string     `json:"connMaxIdleTime"`
	ConnMaxLifetime string     `json:"connMaxLifetime"`
	SSL             *SSLConfig `json:"ssl"`
}

func (config *Config) Make() (client rueidis.Client, err error) {

	return
}

type SentinelConfig struct {
	Enable     bool   `json:"enable"`
	MasterSet  string `json:"masterSet"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	ClientName string `json:"clientName"`
}
