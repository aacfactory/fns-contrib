package configs

import "time"

type MetaConfig struct {
	MaxAge time.Duration `json:"maxAge"`
	MinAge time.Duration `json:"minAge"`
}
