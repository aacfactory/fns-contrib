package http3_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/afssl"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/http/http3"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"net/http"
	"testing"
	"time"
)

func TestHttp3(t *testing.T) {
	ca, caKey, caErr := afssl.CreateCA("FNS-TEST", 365)
	if caErr != nil {
		t.Errorf("%+v", caErr)
		return
	}
	srv1, srv1Err := instance(ca, caKey)
	if srv1Err != nil {
		t.Errorf("%+v", srv1Err)
		return
	}
	srv2, srv2Err := instance(ca, caKey)
	if srv2Err != nil {
		t.Errorf("%+v", srv2Err)
		return
	}
	srvErr := make(chan error)
	go func(srv service.Http, srvErr chan error) {
		srvErr <- srv.ListenAndServe()
	}(srv1, srvErr)
	select {
	case <-time.After(2 * time.Second):
		break
	case sErr := <-srvErr:
		t.Errorf("%+v", sErr)
		return
	}
	client, dialErr := srv2.Dial("127.0.0.1:18080")
	if dialErr != nil {
		_ = srv1.Close()
		t.Errorf("%+v", dialErr)
		return
	}
	status, header, body, getErr := client.Get(context.TODO(), "/hello", http.Header{})
	if getErr != nil {
		_ = srv1.Close()
		t.Errorf("%+v", getErr)
		return
	}
	client.Close()

	fmt.Println(status)
	fmt.Println(header)
	fmt.Println(string(body))

	_ = srv1.Close()

}

func instance(ca []byte, key []byte) (srv service.Http, err error) {
	srvTLS, cliTLS, tlsErr := afssl.SSC(ca, key)
	if tlsErr != nil {
		err = errors.Warning("http3: create ssl failed").WithCause(tlsErr)
		return
	}
	log, logErr := Log()
	if logErr != nil {
		err = logErr
		return
	}
	srv = http3.Server()
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
		err = buildErr
		return
	}
	return
}

func TestSTD(t *testing.T) {
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
	srvTLS.ClientAuth = tls.NoClientCert
	config := http3.Config{
		EnableDatagrams:    false,
		MaxHeaderBytes:     "",
		AdditionalSettings: nil,
		Quic:               nil,
		Client:             nil,
	}
	options, encodeErr := json.Marshal(config)
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	srv := http3.Compatible(&service.FastHttp{})
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
		Options: options,
	})
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	srvErr := srv.ListenAndServe()
	if srvErr != nil {
		t.Errorf("%+v", srvErr)
		return
	}
}

func SSL() (srv *tls.Config, cli *tls.Config, err error) {
	ca, caKey, caErr := afssl.CreateCA("FNS-TEST", 365)
	if caErr != nil {
		err = errors.Warning("http3: create ca failed").WithCause(caErr)
		return
	}
	srv, cli, err = afssl.SSC(ca, caKey)
	if err != nil {
		err = errors.Warning("http3: create ssl failed").WithCause(err)
		return
	}
	return
}

func Log() (log logs.Logger, err error) {
	log, err = logs.New(logs.Name("http3"), logs.Color(true), logs.WithLevel(logs.DebugLevel))
	if err != nil {
		err = errors.Warning("http3: create log failed").WithCause(err)
		return
	}
	return
}
