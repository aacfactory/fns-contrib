package configs

import "github.com/aacfactory/afssl/configs"

type SSLConfig struct {
	Enable bool `json:"enable"`
	configs.Client
}
