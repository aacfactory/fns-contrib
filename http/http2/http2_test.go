package http2_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/afssl"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/http/http2"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"net/http"
	"testing"
	"time"
)

func TestHttp2(t *testing.T) {
	log, logErr := Log()
	if logErr != nil {
		t.Errorf("%+v", logErr)
		return
	}
	srvTLS, cliTLS, tlsErr := SSL()
	if tlsErr != nil {
		t.Errorf("%+v", tlsErr)
		return
	}
	srv := http2.Server()
	buildErr := srv.Build(service.HttpOptions{
		Port:      18080,
		ServerTLS: srvTLS,
		ClientTLS: cliTLS,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(200)
			_, _ = writer.Write([]byte(time.Now().Format(time.RFC3339Nano)))
			return
		}),
		Log:     log,
		Options: nil,
	})
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	srvErr := make(chan error)
	go func(srv service.Http, srvErr chan error) {
		srvErr <- srv.ListenAndServe()
	}(srv, srvErr)
	select {
	case <-time.After(2 * time.Second):
		break
	case sErr := <-srvErr:
		t.Errorf("%+v", sErr)
		return
	}
	client, dialErr := srv.Dial("127.0.0.1:18080")
	if dialErr != nil {
		_ = srv.Close()
		t.Errorf("%+v", dialErr)
		return
	}
	status, header, body, getErr := client.Get(context.TODO(), "/hello", http.Header{})
	if getErr != nil {
		_ = srv.Close()
		t.Errorf("%+v", getErr)
		return
	}
	client.Close()

	fmt.Println(status)
	fmt.Println(header)
	fmt.Println(string(body))

	_ = srv.Close()
}

func SSL() (srv *tls.Config, cli *tls.Config, err error) {
	ca, caKey, caErr := afssl.CreateCA("FNS-TEST", 365)
	if caErr != nil {
		err = errors.Warning("http2: create ca failed").WithCause(caErr)
		return
	}
	srv, cli, err = afssl.SSC(ca, caKey)
	if err != nil {
		err = errors.Warning("http2: create ssl failed").WithCause(err)
		return
	}
	return
}

func Log() (log logs.Logger, err error) {
	log, err = logs.New(logs.Name("http2"), logs.Color(true), logs.WithLevel(logs.DebugLevel))
	if err != nil {
		err = errors.Warning("http2: create log failed").WithCause(err)
		return
	}
	return
}
