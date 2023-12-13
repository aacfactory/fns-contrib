package configs

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

type Config struct {
	Addr      []string        `json:"addr"`
	Username  string          `json:"username"`
	Password  string          `json:"password"`
	SSL       SSLConfig       `json:"ssl"`
	KeepAlive KeepAliveConfig `json:"keepAlive"`
	Shared    json.RawMessage `json:"shared"`
	Barrier   json.RawMessage `json:"barrier"`
}

func (config *Config) As() (v hazelcast.Config, err error) {
	v = hazelcast.NewConfig()
	v.Cluster.Network.SetAddresses(config.Addr...)
	v.Cluster.Security.Credentials.Username = config.Username
	v.Cluster.Security.Credentials.Password = config.Password
	if config.SSL.Enable {
		tlsConfig, tlsErr := config.SSL.Config()
		if tlsErr != nil {
			err = tlsErr
			return
		}
		v.Cluster.Network.SSL.Enabled = true
		v.Cluster.Network.SSL.SetTLSConfig(tlsConfig)
	}
	return
}

func (config *Config) SharedConfig() (v configures.Config, err error) {
	if !json.Validate(config.Shared) {
		config.Shared = []byte{'{', '}'}
	}
	v, err = configures.NewJsonConfig(config.Shared)
	if err != nil {
		err = errors.Warning("hazelcast: get shared config failed").WithCause(err)
		return
	}
	return
}

func (config *Config) BarrierConfig() (v configures.Config, err error) {
	if !json.Validate(config.Barrier) {
		config.Barrier = []byte{'{', '}'}
	}
	v, err = configures.NewJsonConfig(config.Barrier)
	if err != nil {
		err = errors.Warning("hazelcast: get barrier config failed").WithCause(err)
		return
	}
	return
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
