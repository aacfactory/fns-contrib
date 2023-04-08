package http3

import (
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/rings"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/sync/singleflight"
	"sync"
	"time"
)

func NewDialer(cliTLS *tls.Config, clientConfig *ClientConfig, enableDatagrams bool, quicConfig *quic.Config, additionalSettings map[uint64]uint64) (dialer *Dialer, err error) {
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
	dialer = &Dialer{
		maxConnsPerHost:        clientConfig.MaxConnectionsPerHost(),
		config:                 cliTLS,
		quicConfig:             quicConfig,
		enableDatagrams:        enableDatagrams,
		additionalSettings:     additionalSettings,
		maxResponseHeaderBytes: int64(maxResponseHeaderBytes),
		timeout:                timeout,
		group:                  &singleflight.Group{},
		clients:                sync.Map{},
	}
	return
}

type Dialer struct {
	maxConnsPerHost        int
	config                 *tls.Config
	quicConfig             *quic.Config
	enableDatagrams        bool
	additionalSettings     map[uint64]uint64
	maxResponseHeaderBytes int64
	timeout                time.Duration
	group                  *singleflight.Group
	clients                sync.Map
}

func (dialer *Dialer) Dial(address string) (client transports.Client, err error) {
	cc, doErr, _ := dialer.group.Do(address, func() (clients interface{}, err error) {
		hosted, has := dialer.clients.Load(address)
		if has {
			clients = hosted
			return
		}
		hosted = dialer.createClients(address)
		dialer.clients.Store(address, hosted)
		clients = hosted
		return
	})
	if doErr != nil {
		err = errors.Warning("http3: dial failed").WithMeta("address", address).WithCause(doErr)
		return
	}
	clients := cc.(*rings.Ring[*Client])
	client = clients.Next()
	return
}

func (dialer *Dialer) createClients(address string) (clients *rings.Ring[*Client]) {
	endpoints := make([]*Client, 0, 1)
	for i := 0; i < dialer.maxConnsPerHost; i++ {
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
		client := NewClient(address, roundTripper, dialer.timeout)
		endpoints = append(endpoints, client)
	}
	clients = rings.New(address, endpoints...)
	return
}

func (dialer *Dialer) Close() {
	dialer.clients.Range(func(key, value any) bool {
		clients, ok := value.(*rings.Ring[*Client])
		if ok {
			n := clients.Len()
			for i := 0; i < n; i++ {
				clients.Next().Close()
			}
		}
		return true
	})
}
