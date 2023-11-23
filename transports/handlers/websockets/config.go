package websockets

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/wildcard"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/json"
	"strings"
)

type Config struct {
	MaxConnections        int                     `json:"maxConnections"`
	HandshakeTimeout      string                  `json:"handshakeTimeout"`
	ReadTimeout           string                  `json:"readTimeout"`
	ReadBufferSize        string                  `json:"readBufferSize"`
	WriteTimeout          string                  `json:"writeTimeout"`
	WriteBufferSize       string                  `json:"writeBufferSize"`
	EnableCompression     bool                    `json:"enableCompression"`
	MaxRequestMessageSize string                  `json:"maxRequestMessageSize"`
	OriginCheckPolicy     OriginCheckPolicyConfig `json:"originCheckPolicy"`
	ConnectionTTL         string                  `json:"connectionTTL"`
}

type OriginCheckPolicyConfig struct {
	Mode    string          `json:"mode"`
	Options json.RawMessage `json:"options"`
}

func (config *OriginCheckPolicyConfig) Build() (fn func(r transports.Request) bool, err error) {
	switch config.Mode {
	case "non":
		fn = func(r transports.Request) bool {
			return true
		}
		break
	case "pattern":
		if config.Options == nil || len(config.Options) == 0 {
			err = errors.Warning("websockets: origin check pattern mode policy need pattern options")
			return
		}
		opt := patternModeOptions{}
		decodeErr := json.Unmarshal(config.Options, &opt)
		if decodeErr != nil {
			err = errors.Warning("websockets: parse origin check pattern mode policy options failed").WithCause(decodeErr)
			return
		}
		pattern := strings.ToLower(strings.TrimSpace(opt.Pattern))
		if pattern == "" {
			err = errors.Warning("websockets: origin check pattern mode policy need valid pattern options")
			return
		}

		fn = func(r transports.Request) bool {
			origin := r.Header().Get(transports.OriginHeaderName)
			if len(origin) == 0 {
				return false
			}
			return wildcard.Match([]byte(pattern), bytes.ToLower(origin))
		}
	case "same":
		// same as default
		break
	default:
		break
	}
	return
}

type patternModeOptions struct {
	Pattern string `json:"pattern"`
}
