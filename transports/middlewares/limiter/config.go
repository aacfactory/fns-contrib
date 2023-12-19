package limiter

import "github.com/aacfactory/json"

type DeviceConfig struct {
	Enable       bool `json:"enable"`
	EverySeconds int  `json:"everySeconds"`
	Burst        int  `json:"burst"`
	CacheSize    int  `json:"cacheSize"`
}

type AlarmConfig struct {
	EverySeconds int             `json:"everySeconds"`
	Burst        int             `json:"burst"`
	Options      json.RawMessage `json:"options"`
}

type Config struct {
	Enable       bool         `json:"enable"`
	EverySeconds int          `json:"everySeconds"`
	Burst        int          `json:"burst"`
	Device       DeviceConfig `json:"device"`
	Alarm        AlarmConfig  `json:"alarm"`
}
