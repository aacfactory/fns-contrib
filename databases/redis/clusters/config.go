package clusters

import (
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"time"
)

type Config struct {
	configs.Config
	Barrier BarrierConfig
}

type BarrierConfig struct {
	TTL time.Duration `json:"ttl"`
}
