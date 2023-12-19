package http3

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/fns/transports/ssl"
	"github.com/aacfactory/json"
	"github.com/quic-go/quic-go/http3"
)

const (
	transportName = "http3"
)

func NewWithAlternative(alternative transports.Transport) transports.Transport {
	return &Transport{
		alternative: alternative,
	}
}

func New() transports.Transport {
	return NewWithAlternative(nil)
}

type Transport struct {
	server      *Server
	dialer      *Dialer
	alternative transports.Transport
}

func (tr *Transport) Name() (name string) {
	name = transportName
	return
}

func (tr *Transport) Construct(options transports.Options) (err error) {
	// log
	log := options.Log.With("transport", transportName)
	// tls
	if options.Config.TLS == nil {
		err = errors.Warning("http3: transport construct failed").WithCause(fmt.Errorf("tls is required")).WithMeta("transport", transportName)
		return
	}
	tlsConfig, tlsConfigErr := options.Config.GetTLS()
	if tlsConfigErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(tlsConfigErr).WithMeta("transport", transportName)
		return
	}
	srvTLS, _ := tlsConfig.Server()
	cliTLS, _ := tlsConfig.Client()

	// handler
	if options.Handler == nil {
		err = errors.Warning("http3: transport construct failed").WithCause(fmt.Errorf("handler is nil")).WithMeta("transport", transportName)
		return
	}

	// port
	port, portErr := options.Config.GetPort()
	if portErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(portErr).WithMeta("transport", transportName)
		return
	}
	// config
	optConfig, optConfigErr := options.Config.OptionsConfig()
	if optConfigErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(optConfigErr).WithMeta("transport", transportName)
		return
	}
	config := Config{}
	configErr := optConfig.As(&config)
	if configErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(configErr).WithMeta("transport", transportName)
		return
	}
	quicConfig, quicConfigErr := config.QuicConfig()
	if quicConfigErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(quicConfigErr)
		return
	}
	// server
	srv, srvErr := newServer(port, srvTLS, config, quicConfig, options.Handler)
	if srvErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(srvErr).WithMeta("transport", transportName)
		return
	}
	tr.server = srv

	// dialer
	clientConfig := config.ClientConfig()
	dialer, dialerErr := NewDialer(cliTLS, clientConfig, config.EnableDatagrams, quicConfig, config.AdditionalSettings)
	if dialerErr != nil {
		err = errors.Warning("http3: transport construct failed").WithCause(dialerErr)
		return
	}
	tr.dialer = dialer

	// alternative
	if tr.alternative != nil {
		if tr.alternative.Name() != config.Alternative.Name {
			err = errors.Warning("http3: transport construct failed").WithCause(errors.Warning("alternative transport was not matched"))
			return
		}
		if config.Alternative.Options == nil || !json.Validate(config.Alternative.Options) {
			config.Alternative.Options = []byte{'{', '}'}
		}

		alternativeTLSConfig := srvTLS.Clone()
		alternativeTLSConfig.NextProtos = nil

		alternativeConfig := transports.Config{
			Port:        port,
			TLS:         transports.FixedTLSConfig(ssl.NewDefaultConfig(alternativeTLSConfig, cliTLS, nil, nil)),
			Options:     config.Alternative.Options,
			Middlewares: nil,
			Handlers:    nil,
		}

		alternativeErr := tr.alternative.Construct(transports.Options{
			Log:     log.With("alternative", tr.alternative.Name()),
			Config:  alternativeConfig,
			Handler: newAlternativeHandler(tr.server.srv, options.Handler),
		})
		if alternativeErr != nil {
			err = errors.Warning("http3: transport construct failed").WithCause(errors.Warning("build alternative failed")).WithCause(alternativeErr)
			return
		}
	}
	return
}

func (tr *Transport) Dial(address []byte) (client transports.Client, err error) {
	client, err = tr.dialer.Dial(address)
	return
}

func (tr *Transport) Port() (port int) {
	port = tr.server.port
	return
}

func (tr *Transport) ListenAndServe() (err error) {
	sErr := make(chan error)
	qErr := make(chan error)
	if tr.alternative != nil {
		go func(alternative transports.Transport, sErr chan error) {
			sErr <- alternative.ListenAndServe()
		}(tr.alternative, sErr)
	}
	go func(quic *http3.Server, qErr chan error) {
		qErr <- quic.ListenAndServe()
	}(tr.server.srv, qErr)
	select {
	case srvErr := <-sErr:
		tr.Shutdown(context.TODO())
		err = errors.Warning("http3: listen and serve failed").WithCause(srvErr).WithMeta("kind", "http")
		break
	case srvErr := <-qErr:
		tr.Shutdown(context.TODO())
		err = errors.Warning("http3: listen and serve failed").WithCause(srvErr).WithMeta("kind", "http3")
		break
	}
	return
}

func (tr *Transport) Shutdown(ctx context.Context) {
	tr.dialer.Close()
	if tr.alternative != nil {
		tr.alternative.Shutdown(ctx)
	}
	_ = tr.server.Shutdown(ctx)
	tr.dialer.Close()
	return
}
