package standard

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"strings"
	"time"
)

type ClientConfig struct {
	MaxConnsPerHost       int    `json:"maxConnsPerHost"`
	MaxResponseHeaderSize string `json:"maxResponseHeaderSize"`
	Timeout               string `json:"timeout"`
	DisableKeepAlive      bool   `json:"disableKeepAlive"`
	MaxIdleConnsPerHost   int    `json:"maxIdleConnsPerHost"`
	IdleConnTimeout       string `json:"idleConnTimeout"`
	TLSHandshakeTimeout   string `json:"tlsHandshakeTimeout"`
	ExpectContinueTimeout string `json:"expectContinueTimeout"`
}

func (config *Config) ClientConfig() *ClientConfig {
	if config.Client == nil {
		return &ClientConfig{}
	}
	return config.Client
}

type Config struct {
	MaxRequestHeaderSize string        `json:"maxRequestHeaderSize"`
	MaxRequestBodySize   string        `json:"maxRequestBodySize"`
	ReadTimeout          string        `json:"readTimeout"`
	ReadHeaderTimeout    string        `json:"readHeaderTimeout"`
	WriteTimeout         string        `json:"writeTimeout"`
	IdleTimeout          string        `json:"idleTimeout"`
	Client               *ClientConfig `json:"client"`
}

func (config *ClientConfig) MaxConnectionsPerHost() (n int) {
	if config.MaxConnsPerHost < 1 {
		config.MaxConnsPerHost = 64
	}
	n = config.MaxConnsPerHost
	return
}

func (config *ClientConfig) MaxIdleConnectionsPerHost() (n int) {
	if config.MaxIdleConnsPerHost < 1 {
		config.MaxIdleConnsPerHost = 100
	}
	n = config.MaxIdleConnsPerHost
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

func (config *ClientConfig) IdleConnTimeoutDuration() (n time.Duration, err error) {
	timeout := strings.TrimSpace(config.IdleConnTimeout)
	if timeout == "" {
		timeout = "90s"
	}
	n, err = time.ParseDuration(timeout)
	if err != nil {
		err = errors.Warning("idle conn timeout is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
		return
	}
	return
}

func (config *ClientConfig) TLSHandshakeTimeoutDuration() (n time.Duration, err error) {
	timeout := strings.TrimSpace(config.TLSHandshakeTimeout)
	if timeout == "" {
		timeout = "10s"
	}
	n, err = time.ParseDuration(timeout)
	if err != nil {
		err = errors.Warning("tls handshake timeout is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
		return
	}
	return
}

func (config *ClientConfig) ExpectContinueTimeoutDuration() (n time.Duration, err error) {
	timeout := strings.TrimSpace(config.ExpectContinueTimeout)
	if timeout == "" {
		timeout = "1s"
	}
	n, err = time.ParseDuration(timeout)
	if err != nil {
		err = errors.Warning("expect continue timeout is invalid").WithCause(err).WithMeta("hit", "format must be time.Duration")
		return
	}
	return
}
