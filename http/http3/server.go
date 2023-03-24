package http3

import (
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/quic-go/quic-go/http3"
	"strings"
)

const (
	httpContentType     = "Content-Type"
	httpContentTypeJson = "application/json"
)

func Server() service.Http {
	return &server{}
}

func Compatible(compatible service.Http) service.Http {
	return &server{
		compatible: compatible,
	}
}

type server struct {
	log        logs.Logger
	compatible service.Http
	quic       *http3.Server
	dialer     *Dialer
}

func (srv *server) Name() (name string) {
	name = "http3"
	return
}

func (srv *server) Build(options service.HttpOptions) (err error) {
	srvTLS := options.ServerTLS
	if srvTLS == nil {
		err = errors.Warning("http3: build failed").WithCause(errors.Warning("tls is required"))
		return
	}
	srv.log = options.Log
	config := Config{}
	decodeErr := options.Options.As(&config)
	if decodeErr != nil {
		err = errors.Warning("http3: build failed").WithCause(decodeErr)
		return
	}
	maxHeaderBytes := uint64(0)
	if config.MaxHeaderBytes != "" {
		maxHeaderBytes, err = bytex.ToBytes(strings.TrimSpace(config.MaxHeaderBytes))
		if err != nil {
			err = errors.Warning("http3: build failed").WithCause(errors.Warning("maxHeaderBytes is invalid").WithCause(err).WithMeta("hit", "format must be bytes"))
			return
		}
	}
	quicConfig, quicConfigErr := config.QuicConfig()
	if quicConfigErr != nil {
		err = errors.Warning("http3: build failed").WithCause(quicConfigErr)
		return
	}
	// server
	handler := options.Handler
	srv.quic = &http3.Server{
		Addr:               fmt.Sprintf(":%d", options.Port),
		Port:               options.Port,
		TLSConfig:          http3.ConfigureTLSConfig(srvTLS),
		QuicConfig:         quicConfig,
		Handler:            handler,
		EnableDatagrams:    config.EnableDatagrams,
		MaxHeaderBytes:     int(maxHeaderBytes),
		AdditionalSettings: config.AdditionalSettings,
		StreamHijacker:     nil,
		UniStreamHijacker:  nil,
	}
	// compatible
	if srv.compatible != nil {
		if config.Compatible == nil || !json.Validate(config.Compatible) {
			config.Compatible = []byte{'{', '}'}
		}
		compatibleConfig, compatibleConfigErr := configures.NewJsonConfig(config.Compatible)
		if compatibleConfigErr != nil {
			err = errors.Warning("http3: build failed").WithCause(compatibleConfigErr)
			return
		}
		compatibleOptions := service.HttpOptions{
			Port:      options.Port,
			ServerTLS: options.ServerTLS,
			ClientTLS: options.ClientTLS,
			Handler:   newCompatibleHandler(srv.quic, options.Handler),
			Log:       options.Log.With("compatible", srv.compatible.Name()),
			Options:   compatibleConfig,
		}
		buildCompatibleErr := srv.compatible.Build(compatibleOptions)
		if buildCompatibleErr != nil {
			err = errors.Warning("http3: build failed").WithCause(errors.Warning("build compatible failed")).WithCause(buildCompatibleErr)
			return
		}
	}
	// dialer
	dialer, dialerErr := NewDialer(options.ClientTLS, config.ClientConfig(), config.EnableDatagrams, quicConfig, config.AdditionalSettings)
	if dialerErr != nil {
		err = errors.Warning("http3: build failed").WithCause(dialerErr)
		return
	}
	srv.dialer = dialer
	return
}

func (srv *server) Dial(address string) (client service.HttpClient, err error) {
	client, err = srv.dialer.Dial(address)
	return
}

func (srv *server) ListenAndServe() (err error) {
	if srv.compatible == nil {
		err = srv.quic.ListenAndServe()
		if err != nil {
			err = errors.Warning("http3: listen and serve failed").WithCause(err)
			return
		}
		return
	}
	sErr := make(chan error)
	qErr := make(chan error)
	go func(compatible service.Http, sErr chan error) {
		sErr <- compatible.ListenAndServe()
	}(srv.compatible, sErr)
	go func(quic *http3.Server, qErr chan error) {
		qErr <- quic.ListenAndServe()
	}(srv.quic, qErr)
	select {
	case srvErr := <-sErr:
		_ = srv.Close()
		err = errors.Warning("http3: listen and serve failed").WithCause(srvErr).WithMeta("kind", "http")
		return
	case srvErr := <-qErr:
		_ = srv.Close()
		err = errors.Warning("http3: listen and serve failed").WithCause(srvErr).WithMeta("kind", "http3")
		return
	}
}

func (srv *server) Close() (err error) {
	srv.dialer.Close()
	errs := errors.MakeErrors()
	if srv.compatible != nil {
		compatibleErr := srv.compatible.Close()
		if compatibleErr != nil {
			errs.Append(compatibleErr)
		}
	}
	quicErr := srv.quic.Close()
	if quicErr != nil {
		errs.Append(quicErr)
	}
	if len(errs) > 0 {
		err = errors.Warning("http3: close failed").WithCause(errs.Error())
	}
	return
}
