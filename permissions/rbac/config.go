package rbac

import "time"

type CacheConfig struct {
	Enable bool          `json:"enable"`
	TTL    time.Duration `json:"ttl"`
}

type Config struct {
	Cache CacheConfig `json:"cache"`
}
