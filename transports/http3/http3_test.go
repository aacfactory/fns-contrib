package http3_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/afssl"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/http3"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
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
	go func(srv transports.Transport, srvErr chan error) {
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
	beg := time.Now()
	resp, doErr := client.Do(context.TODO(), transports.NewUnsafeRequest(context.TODO(), transports.MethodGET, []byte("/hello")))
	fmt.Println("cost:", time.Now().Sub(beg))
	if doErr != nil {
		_ = srv1.Close()
		t.Errorf("%+v", doErr)
		return
	}
	client.Close()

	fmt.Println(resp.Status)
	fmt.Println(resp.Header)
	fmt.Println(string(resp.Body))

	_ = srv1.Close()

}

func instance(ca []byte, key []byte) (srv transports.Transport, err error) {
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
	options, _ := configures.NewJsonConfig([]byte{'{', '}'})
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
		Config: options,
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
		MaxBodyBytes:       "",
		AdditionalSettings: nil,
		Quic:               nil,
		Client:             nil,
		Compatible: &http3.CompatibleConfig{
			Name:    "fasthttp",
			Options: nil,
		},
	}
	options, encodeErr := json.Marshal(config)
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	optionsConfig, _ := configures.NewJsonConfig(options)
	srv := http3.Server()
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
		Config: optionsConfig,
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
