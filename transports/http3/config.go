package http3

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/json"
	"github.com/quic-go/quic-go"
	"strings"
	"time"
)

type Config struct {
	EnableDatagrams      bool               `json:"enableDatagrams"`
	MaxRequestHeaderSize string             `json:"maxRequestHeaderSize"`
	MaxRequestBodySize   string             `json:"maxRequestBodySize"`
	AdditionalSettings   map[uint64]uint64  `json:"additionalSettings"`
	Quic                 *QuicConfig        `json:"quic"`
	Client               *ClientConfig      `json:"client"`
	Alternative          *AlternativeConfig `json:"alternative"`
}

func (config *Config) QuicConfig() (quicConfig *quic.Config, err error) {
	if config.Quic == nil {
		return
	}
	quicConfig, err = config.Quic.Convert(config.EnableDatagrams)
	return
}

func (config *Config) ClientConfig() (clientConfig *ClientConfig) {
	if config.Client == nil {
		clientConfig = &ClientConfig{}
		return
	}
	clientConfig = config.Client
	return
}

type AlternativeConfig struct {
	Name    string          `json:"name"`
	Options json.RawMessage `json:"options"`
}

type ClientConfig struct {
	MaxConnsPerHost       int    `json:"maxConnsPerHost"`
	MaxResponseHeaderSize string `json:"maxResponseHeaderSize"`
	Timeout               string `json:"timeout"`
}

func (config *ClientConfig) MaxConnectionsPerHost() (n int) {
	if config.MaxConnsPerHost < 1 {
		config.MaxConnsPerHost = 64
	}
	n = config.MaxConnsPerHost
	return
}

func (config *ClientConfig) MaxResponseHeaderByteSize() (n uint64, err error) {
	maxResponseHeaderSize := strings.TrimSpace(config.MaxResponseHeaderSize)
	if maxResponseHeaderSize == "" {
		maxResponseHeaderSize = "4KB"
	}
	n, err = bytex.ParseBytes(maxResponseHeaderSize)
	if err != nil {
		err = errors.Warning("maxResponseHeaderBytes is invalid").WithCause(err).WithMeta("hit", "format must be bytes")
		return
	}
	return
}

func (config *ClientConfig) TimeoutDuration() (n time.Duration, err error) {
	timeout := strings.TrimSpace(config.Timeout)
	if timeout == "" {
		timeout = "2s"
	}
	n, err = time.ParseDuration(timeout)
	if err != nil {
		err = errors.Warning("timeout is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
		return
	}
	return
}

type QuicConfig struct {
	Versions                         []string `json:"versions"`
	HandshakeIdleTimeout             string   `json:"handshakeIdleTimeout"`
	MaxIdleTimeout                   string   `json:"maxIdleTimeout"`
	MaxRetryTokenAge                 string   `json:"maxRetryTokenAge"`
	MaxTokenAge                      string   `json:"maxTokenAge"`
	InitialStreamReceiveWindow       string   `json:"initialStreamReceiveWindow"`
	MaxStreamReceiveWindow           string   `json:"maxStreamReceiveWindow"`
	InitialConnectionReceiveWindow   string   `json:"initialConnectionReceiveWindow"`
	MaxConnectionReceiveWindow       string   `json:"maxConnectionReceiveWindow"`
	MaxIncomingStreams               int64    `json:"maxIncomingStreams"`
	MaxIncomingUniStreams            int64    `json:"maxIncomingUniStreams"`
	StatelessResetKey                string   `json:"statelessResetKey"`
	KeepAlivePeriod                  string   `json:"keepAlivePeriod"`
	DisablePathMTUDiscovery          bool     `json:"disablePathMtuDiscovery"`
	DisableVersionNegotiationPackets bool     `json:"disableVersionNegotiationPackets"`
}

func (config *QuicConfig) Convert(enableDatagrams bool) (quicConfig *quic.Config, err error) {
	var versions []quic.VersionNumber
	if config.Versions != nil && len(config.Versions) > 0 {
		versions = make([]quic.VersionNumber, 0, 1)
		for _, version := range config.Versions {
			version = strings.ToUpper(strings.TrimSpace(version))
			switch version {
			case "V1":
				versions = append(versions, quic.Version1)
				break
			case "V2":
				versions = append(versions, quic.Version2)
				break
			case "DRAFT29":
				versions = append(versions, quic.VersionDraft29)
				break
			default:
				break
			}
		}
		if len(versions) == 0 {
			err = errors.Warning("versions is invalid").WithMeta("hit", "see quic-go for more details")
			return
		}
	}
	handshakeIdleTimeout := time.Duration(0)
	if config.HandshakeIdleTimeout != "" {
		handshakeIdleTimeout, err = time.ParseDuration(strings.TrimSpace(config.HandshakeIdleTimeout))
		if err != nil {
			err = errors.Warning("handshakeIdleTimeout is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
			return
		}
	}
	maxIdleTimeout := time.Duration(0)
	if config.MaxIdleTimeout != "" {
		maxIdleTimeout, err = time.ParseDuration(strings.TrimSpace(config.MaxIdleTimeout))
		if err != nil {
			err = errors.Warning("maxIdleTimeout is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
			return
		}
	}
	maxRetryTokenAge := time.Duration(0)
	if config.MaxRetryTokenAge != "" {
		maxIdleTimeout, err = time.ParseDuration(strings.TrimSpace(config.MaxRetryTokenAge))
		if err != nil {
			err = errors.Warning("maxRetryTokenAge is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
			return
		}
	}
	maxTokenAge := time.Duration(0)
	if config.MaxTokenAge != "" {
		maxTokenAge, err = time.ParseDuration(strings.TrimSpace(config.MaxTokenAge))
		if err != nil {
			err = errors.Warning("maxTokenAge is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
			return
		}
	}
	initialStreamReceiveWindow := uint64(0)
	if config.InitialStreamReceiveWindow != "" {
		initialStreamReceiveWindow, err = bytex.ParseBytes(strings.TrimSpace(config.InitialStreamReceiveWindow))
		if err != nil {
			err = errors.Warning("initialStreamReceiveWindow is invalid").WithCause(err).WithMeta("hit", "format must be bytes")
			return
		}
	}
	maxStreamReceiveWindow := uint64(0)
	if config.MaxStreamReceiveWindow != "" {
		maxStreamReceiveWindow, err = bytex.ParseBytes(strings.TrimSpace(config.MaxStreamReceiveWindow))
		if err != nil {
			err = errors.Warning("maxStreamReceiveWindow is invalid").WithCause(err).WithMeta("hit", "format must be bytes")
			return
		}
	}
	initialConnectionReceiveWindow := uint64(0)
	if config.InitialConnectionReceiveWindow != "" {
		initialConnectionReceiveWindow, err = bytex.ParseBytes(strings.TrimSpace(config.InitialConnectionReceiveWindow))
		if err != nil {
			err = errors.Warning("initialConnectionReceiveWindow is invalid").WithCause(err).WithMeta("hit", "format must be bytes")
			return
		}
	}
	maxConnectionReceiveWindow := uint64(0)
	if config.MaxConnectionReceiveWindow != "" {
		maxConnectionReceiveWindow, err = bytex.ParseBytes(strings.TrimSpace(config.MaxConnectionReceiveWindow))
		if err != nil {
			err = errors.Warning("maxConnectionReceiveWindow is invalid").WithCause(err).WithMeta("hit", "format must be bytes")
			return
		}
	}
	var statelessResetKey *quic.StatelessResetKey
	if config.StatelessResetKey != "" {
		if len(config.StatelessResetKey) != 32 {
			err = errors.Warning("statelessResetKey is invalid").WithCause(err).WithMeta("hit", "see quic-go for more details")
			return
		}
		p := make([]byte, 32)
		copy(p, config.StatelessResetKey)
		key := quic.StatelessResetKey(p)
		statelessResetKey = &key
	}
	keepAlivePeriod := time.Duration(0)
	if config.KeepAlivePeriod != "" {
		keepAlivePeriod, err = time.ParseDuration(strings.TrimSpace(config.KeepAlivePeriod))
		if err != nil {
			err = errors.Warning("keepAlivePeriod is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
			return
		}
	}
	quicConfig = &quic.Config{
		Versions:                         versions,
		ConnectionIDLength:               0,
		ConnectionIDGenerator:            &ConnectionIDGenerator{},
		HandshakeIdleTimeout:             handshakeIdleTimeout,
		MaxIdleTimeout:                   maxIdleTimeout,
		RequireAddressValidation:         nil,
		MaxRetryTokenAge:                 maxRetryTokenAge,
		MaxTokenAge:                      maxTokenAge,
		TokenStore:                       nil,
		InitialStreamReceiveWindow:       initialStreamReceiveWindow,
		MaxStreamReceiveWindow:           maxStreamReceiveWindow,
		InitialConnectionReceiveWindow:   initialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:       maxConnectionReceiveWindow,
		AllowConnectionWindowIncrease:    nil,
		MaxIncomingStreams:               config.MaxIncomingStreams,
		MaxIncomingUniStreams:            config.MaxIncomingUniStreams,
		StatelessResetKey:                statelessResetKey,
		KeepAlivePeriod:                  keepAlivePeriod,
		DisablePathMTUDiscovery:          config.DisablePathMTUDiscovery,
		DisableVersionNegotiationPackets: config.DisableVersionNegotiationPackets,
		Allow0RTT:                        nil,
		EnableDatagrams:                  enableDatagrams,
		Tracer:                           nil,
	}
	return
}
