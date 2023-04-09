package fasthttp2

import (
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service/transports"
	"golang.org/x/sync/singleflight"
	"sync"
)

func NewDialer(cliTLS *tls.Config, config *transports.FastHttpClientOptions) (dialer *Dialer, err error) {
	opts, optsErr := NewClientOptions(config)
	if optsErr != nil {
		err = errors.Warning("create dialer failed").WithCause(optsErr)
		return
	}
	dialer = &Dialer{
		config:    cliTLS,
		clientOpt: opts,
		group:     &singleflight.Group{},
		clients:   sync.Map{},
	}
	return
}

type Dialer struct {
	config    *tls.Config
	clientOpt *ClientOptions
	group     *singleflight.Group
	clients   sync.Map
}

func (dialer *Dialer) Dial(address string) (client transports.Client, err error) {
	cc, doErr, _ := dialer.group.Do(address, func() (clients interface{}, err error) {
		hosted, has := dialer.clients.Load(address)
		if has {
			clients = hosted
			return
		}
		hosted, err = dialer.createClient(address)
		if err != nil {
			return
		}
		dialer.clients.Store(address, hosted)
		clients = hosted
		return
	})
	if doErr != nil {
		err = errors.Warning("http2: dial failed").WithMeta("address", address).WithCause(doErr)
		return
	}
	client = cc.(*Client)
	return
}

func (dialer *Dialer) createClient(address string) (client *Client, err error) {
	client, err = NewClient(address, dialer.config, dialer.clientOpt)
	if err != nil {
		return
	}
	return
}

func (dialer *Dialer) Close() {
	dialer.clients.Range(func(key, value any) bool {
		client, ok := value.(*Client)
		if ok {
			client.Close()
		}
		return true
	})
}
