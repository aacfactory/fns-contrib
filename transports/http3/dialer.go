package http3

import (
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/caches/lru"
	"github.com/aacfactory/fns/transports"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/sync/singleflight"
	"time"
)

func NewDialer(cliTLS *tls.Config, clientConfig ClientConfig, enableDatagrams bool, quicConfig *quic.Config, additionalSettings map[uint64]uint64) (dialer *Dialer, err error) {
	maxResponseHeaderBytes, maxResponseHeaderBytesErr := clientConfig.MaxResponseHeaderByteSize()
	if maxResponseHeaderBytesErr != nil {
		err = maxResponseHeaderBytesErr
		return
	}
	timeout, timeoutErr := clientConfig.TimeoutDuration()
	if timeoutErr != nil {
		err = timeoutErr
		return
	}
	cacheSize := clientConfig.Dialer.CacheSize
	if cacheSize < 1 {
		cacheSize = 64
	}
	cacheSeconds := clientConfig.Dialer.ExpireSeconds
	if cacheSeconds < 1 {
		cacheSeconds = 24 * 60 * 60
	}
	dialer = &Dialer{
		config:                 cliTLS,
		quicConfig:             quicConfig,
		enableDatagrams:        enableDatagrams,
		additionalSettings:     additionalSettings,
		maxResponseHeaderBytes: int64(maxResponseHeaderBytes),
		timeout:                timeout,
		group:                  &singleflight.Group{},
		clients: lru.NewWithExpire[string, transports.Client](cacheSize, time.Duration(cacheSeconds)*time.Second, func(key string, value transports.Client) {
			value.Close()
		}),
	}
	return
}

type Dialer struct {
	config                 *tls.Config
	quicConfig             *quic.Config
	enableDatagrams        bool
	additionalSettings     map[uint64]uint64
	maxResponseHeaderBytes int64
	timeout                time.Duration
	group                  *singleflight.Group
	clients                *lru.LRU[string, transports.Client]
}

func (dialer *Dialer) Dial(addressBytes []byte) (client transports.Client, err error) {
	address := bytex.ToString(addressBytes)
	cc, doErr, _ := dialer.group.Do(address, func() (client interface{}, err error) {
		hosted, has := dialer.clients.Get(address)
		if has {
			client = hosted
			return
		}
		hosted = dialer.createClient(address)
		dialer.clients.Add(address, hosted)
		client = hosted
		return
	})
	if doErr != nil {
		err = errors.Warning("http3: dial failed").WithMeta("address", address).WithCause(doErr)
		return
	}
	client = cc.(*Client)
	return
}

func (dialer *Dialer) createClient(address string) (client *Client) {
	roundTripper := &http3.RoundTripper{
		DisableCompression:     false,
		TLSClientConfig:        dialer.config,
		QuicConfig:             dialer.quicConfig,
		EnableDatagrams:        dialer.enableDatagrams,
		AdditionalSettings:     dialer.additionalSettings,
		StreamHijacker:         nil,
		UniStreamHijacker:      nil,
		Dial:                   nil,
		MaxResponseHeaderBytes: dialer.maxResponseHeaderBytes,
	}
	client = NewClient(address, roundTripper, dialer.timeout)
	return
}

func (dialer *Dialer) Close() {
	dialer.clients.Purge()
}
