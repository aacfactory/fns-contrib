package fasthttp2_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/afssl"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/fasthttp2"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/logs"
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
	srv := fasthttp2.Server()
	opts := `{"prefork":false}`
	opt, _ := configures.NewJsonConfig([]byte(opts))
	buildErr := srv.Build(transports.Options{
		Port:      18080,
		ServerTLS: srvTLS,
		ClientTLS: cliTLS,
		Handler: transports.HandlerFunc(func(writer transports.ResponseWriter, request *transports.Request) {
			writer.SetStatus(200)
			_, _ = writer.Write([]byte(time.Now().Format(time.RFC3339Nano)))
			return
		}),
		Log:    log,
		Config: opt,
	})
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	srvErr := make(chan error)
	go func(srv transports.Transport, srvErr chan error) {
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
	beg := time.Now()
	resp, doErr := client.Do(context.TODO(), transports.NewUnsafeRequest(context.TODO(), transports.MethodGET, []byte("/hello")))
	fmt.Println("cost:", time.Now().Sub(beg))
	if doErr != nil {
		_ = srv.Close()
		t.Errorf("%+v", doErr)
		return
	}
	client.Close()

	fmt.Println(resp.Status)
	fmt.Println("-----")
	for name, value := range resp.Header {
		fmt.Println("header:", name, value)
	}
	fmt.Println("-----")
	fmt.Println(string(resp.Body))

	_ = srv.Close()
}

func TestServer_ListenAndServe(t *testing.T) {
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
	srv := fasthttp2.Server()
	opt, _ := configures.NewJsonConfig([]byte{'{', '}'})
	buildErr := srv.Build(transports.Options{
		Port:      18080,
		ServerTLS: srvTLS,
		ClientTLS: cliTLS,
		Handler: transports.HandlerFunc(func(writer transports.ResponseWriter, request *transports.Request) {
			writer.SetStatus(200)
			_, _ = writer.Write([]byte(time.Now().Format(time.RFC3339Nano)))
			return
		}),
		Log:    log,
		Config: opt,
	})
	if buildErr != nil {
		t.Errorf("%+v", buildErr)
		return
	}
	err := srv.ListenAndServe()
	if err != nil {
		t.Errorf("%+v", err)
	}
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
