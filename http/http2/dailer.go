package http2

import (
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/rings"
	"golang.org/x/sync/singleflight"
	"sync"
)

func NewDialer(cliTLS *tls.Config, config *service.FastHttpClientOptions) (dialer *Dialer, err error) {
	opts, optsErr := NewClientOptions(config)
	if optsErr != nil {
		err = errors.Warning("create dialer failed").WithCause(optsErr)
		return
	}
	maxConnsPerHost := opts.MaxConns
	if maxConnsPerHost < 1 {
		maxConnsPerHost = 64
	}
	dialer = &Dialer{
		maxConnsPerHost: maxConnsPerHost,
		config:          cliTLS,
		clientOpt:       opts,
		group:           &singleflight.Group{},
		clients:         sync.Map{},
	}
	return
}

type Dialer struct {
	maxConnsPerHost int
	config          *tls.Config
	clientOpt       *ClientOptions
	group           *singleflight.Group
	clients         sync.Map
}

func (dialer *Dialer) Dial(address string) (client service.HttpClient, err error) {
	cc, doErr, _ := dialer.group.Do(address, func() (clients interface{}, err error) {
		hosted, has := dialer.clients.Load(address)
		if has {
			clients = hosted
			return
		}
		hosted, err = dialer.createClients(address)
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
	clients := cc.(*rings.Ring[*Client])
	client = clients.Next()
	return
}

func (dialer *Dialer) createClients(address string) (clients *rings.Ring[*Client], err error) {
	endpoints := make([]*Client, 0, 1)
	for i := 0; i < dialer.maxConnsPerHost; i++ {
		client, clientErr := NewClient(address, dialer.config, dialer.clientOpt)
		if clientErr != nil {
			err = clientErr
			return
		}
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
