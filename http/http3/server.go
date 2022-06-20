package http3

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/server"
	"github.com/aacfactory/logs"
	"github.com/lucas-clemente/quic-go/http3"
	"log"
	"net"
	"net/http"
)

const (
	httpContentType     = "Content-Type"
	httpContentTypeJson = "application/json"
)

func Server() server.Http {
	return &quicServer{}
}

type quicServer struct {
	log     logs.Logger
	udpConn *net.UDPConn
	tcpConn net.Listener
	ss      *http.Server
	qs      *http3.Server
}

func (srv *quicServer) Build(options server.HttpOptions) (err error) {
	srv.log = options.Log
	if options.ServerTLS == nil {
		err = errors.Warning("fns: build http3 quicServer failed").WithCause(fmt.Errorf("http3 need tls"))
		return
	}
	udpAddr, udpAddrErr := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", options.Port))
	if udpAddrErr != nil {
		err = errors.Warning("fns: build http3 quicServer failed").WithCause(udpAddrErr)
		return
	}
	udpConn, udpConnErr := net.ListenUDP("udp", udpAddr)
	if udpConnErr != nil {
		err = errors.Warning("fns: build http3 quicServer failed").WithCause(udpConnErr)
		return
	}
	tcpAddr, tcpAddrErr := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", options.Port))
	if tcpAddrErr != nil {
		err = errors.Warning("fns: build http3 quicServer failed").WithCause(tcpAddrErr)
		return
	}
	tcpConn, tcpConnErr := net.ListenTCP("tcp", tcpAddr)
	if tcpConnErr != nil {
		err = errors.Warning("fns: build http3 quicServer failed").WithCause(tcpConnErr)
		return
	}
	tlsConn := tls.NewListener(tcpConn, options.ServerTLS)
	httpServer := &http.Server{
		ErrorLog: log.New(&Printf{
			Core: options.Log,
		}, "", log.LstdFlags),
	}
	qsServer := &http3.Server{
		Server: &http.Server{
			TLSConfig: options.ServerTLS,
			Handler:   options.Handler,
			ErrorLog: log.New(&Printf{
				Core: options.Log,
			}, "", log.LstdFlags),
		},
	}
	httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerErr := qsServer.SetQuicHeaders(w.Header())
		if headerErr != nil {
			errorHandler(w, headerErr)
			return
		}
		options.Handler.ServeHTTP(w, r)
	})
	srv.udpConn = udpConn
	srv.tcpConn = tlsConn
	srv.ss = httpServer
	srv.qs = qsServer
	return
}

func (srv *quicServer) ListenAndServe() (err error) {
	hErr := make(chan error)
	qErr := make(chan error)
	go func() {
		hErr <- srv.ss.Serve(srv.tcpConn)
	}()
	go func() {
		qErr <- srv.qs.Serve(srv.udpConn)
	}()
	select {
	case srvErr := <-hErr:
		_ = srv.qs.Close()
		err = errors.Warning("fns: quicServer listen and serve failed").WithCause(err).WithMeta("fns", "http3").WithMeta("kind", "tcp").WithCause(srvErr)
		return
	case srvErr := <-qErr:
		err = errors.Warning("fns: quicServer listen and serve failed").WithCause(err).WithMeta("fns", "http3").WithMeta("kind", "udp").WithCause(srvErr)
		return
	}
}

func (srv *quicServer) Close() (err error) {
	var closeErr errors.CodeError
	ssErr := srv.ss.Shutdown(context.TODO())
	if ssErr != nil {
		closeErr = errors.Warning("fns: quicServer close failed").WithCause(err).WithMeta("fns", "http").WithMeta("kind", "tcp")
	}
	qsErr := srv.qs.Close()
	if qsErr != nil {
		if closeErr == nil {
			closeErr = errors.Warning("fns: quicServer close failed").WithCause(err).WithMeta("fns", "http").WithMeta("kind", "udp")
		} else {
			closeErr = errors.Warning("fns: quicServer close failed").WithCause(err).WithMeta("fns", "http").WithMeta("kind", "udp").WithCause(closeErr)

		}
	}
	if closeErr != nil {
		err = closeErr
	}
	return
}

func errorHandler(writer http.ResponseWriter, err error) {
	writer.WriteHeader(555)
	writer.Header().Set(httpContentType, httpContentTypeJson)
	_, _ = writer.Write([]byte(fmt.Sprintf("{\"error\": \"%s\"}", err.Error())))
}
