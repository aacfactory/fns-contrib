package standard

import (
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service/transports"
	"golang.org/x/sync/singleflight"
	"net"
	"net/http"
	"sync"
	"time"
)

func NewDialer(cliTLS *tls.Config, clientConfig *ClientConfig) (dialer *Dialer, err error) {
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
	idleConnTimeout, idleConnTimeoutErr := clientConfig.IdleConnTimeoutDuration()
	if idleConnTimeoutErr != nil {
		err = idleConnTimeoutErr
		return
	}
	tlsHandshakeTimeout, tlsHandshakeTimeoutErr := clientConfig.TLSHandshakeTimeoutDuration()
	if tlsHandshakeTimeoutErr != nil {
		err = tlsHandshakeTimeoutErr
		return
	}
	expectContinueTimeout, expectContinueTimeoutErr := clientConfig.ExpectContinueTimeoutDuration()
	if expectContinueTimeoutErr != nil {
		err = expectContinueTimeoutErr
		return
	}
	dialer = &Dialer{
		config:                cliTLS,
		maxConnsPerHost:       clientConfig.MaxConnectionsPerHost(),
		maxResponseHeaderSize: int64(maxResponseHeaderBytes),
		timeout:               timeout,
		disableKeepAlive:      clientConfig.DisableKeepAlive,
		maxIdleConnsPerHost:   clientConfig.MaxIdleConnectionsPerHost(),
		idleConnTimeout:       idleConnTimeout,
		tlsHandshakeTimeout:   tlsHandshakeTimeout,
		expectContinueTimeout: expectContinueTimeout,
		group:                 &singleflight.Group{},
		clients:               sync.Map{},
	}
	return
}

type Dialer struct {
	config                *tls.Config
	maxConnsPerHost       int
	maxResponseHeaderSize int64
	timeout               time.Duration
	disableKeepAlive      bool
	maxIdleConnsPerHost   int
	idleConnTimeout       time.Duration
	tlsHandshakeTimeout   time.Duration
	expectContinueTimeout time.Duration
	group                 *singleflight.Group
	clients               sync.Map
}

func (dialer *Dialer) Dial(address string) (client transports.Client, err error) {
	cc, doErr, _ := dialer.group.Do(address, func() (client interface{}, err error) {
		hosted, has := dialer.clients.Load(address)
		if has {
			client = hosted
			return
		}
		hosted = dialer.createClient(address)
		dialer.clients.Store(address, hosted)
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
	netDialer := &net.Dialer{
		Timeout:   dialer.timeout,
		KeepAlive: 30 * time.Second,
	}
	roundTripper := &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            netDialer.DialContext,
		DialTLSContext:         nil,
		TLSClientConfig:        dialer.config,
		TLSHandshakeTimeout:    dialer.tlsHandshakeTimeout,
		DisableKeepAlives:      dialer.disableKeepAlive,
		DisableCompression:     false,
		MaxIdleConns:           dialer.maxIdleConnsPerHost,
		MaxIdleConnsPerHost:    dialer.maxIdleConnsPerHost,
		MaxConnsPerHost:        dialer.maxConnsPerHost,
		IdleConnTimeout:        dialer.idleConnTimeout,
		ResponseHeaderTimeout:  0,
		ExpectContinueTimeout:  dialer.expectContinueTimeout,
		TLSNextProto:           nil,
		MaxResponseHeaderBytes: dialer.maxResponseHeaderSize,
		WriteBufferSize:        4096,
		ReadBufferSize:         4096,
		ForceAttemptHTTP2:      true,
	}
	client = NewClient(address, roundTripper, dialer.timeout)
	return
}

func (dialer *Dialer) Close() {
	dialer.clients.Range(func(key, value any) bool {
		client := value.(*Client)
		client.Close()
		return true
	})
}
