package clusters

import (
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"time"
)

type Config struct {
	configs.Config
	KeepAlive KeepAliveConfig `json:"keepAlive" yaml:"keepAlive"`
	Barrier   BarrierConfig   `json:"barrier" yaml:"barrier"`
}

type KeepAliveConfig struct {
	TTL      time.Duration `json:"ttl"`
	Interval time.Duration `json:"interval"`
}

func (config *KeepAliveConfig) GetTTL() time.Duration {
	if config.TTL < 1*time.Second {
		config.TTL = 60 * time.Second
	}
	return config.TTL
}

func (config *KeepAliveConfig) GetInterval() time.Duration {
	if config.Interval <= config.TTL {
		config.Interval = config.TTL / 2
	}
	return config.Interval
}

type BarrierConfig struct {
	TTL time.Duration `json:"ttl"`
}
