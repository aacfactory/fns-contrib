package http3_test

import (
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/afssl"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/http3"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/fns/transports/fast"
	"github.com/aacfactory/fns/transports/ssl"
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
	client, dialErr := srv2.Dial([]byte("127.0.0.1:18080"))
	if dialErr != nil {
		srv1.Shutdown(context.TODO())
		t.Errorf("%+v", dialErr)
		return
	}
	beg := time.Now()
	status, _, respBody, doErr := client.Do(context.TODO(), transports.MethodGet, []byte("/hello"), nil, nil)
	fmt.Println("cost:", time.Now().Sub(beg))
	if doErr != nil {
		srv1.Shutdown(context.TODO())
		t.Errorf("%+v", doErr)
		return
	}
	client.Close()

	fmt.Println(status)
	fmt.Println(string(respBody))

	srv1.Shutdown(context.TODO())

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
	handler := transports.HandlerFunc(func(w transports.ResponseWriter, r transports.Request) {
		fmt.Println("proto:", string(r.Proto()))
		w.SetStatus(200)
		_, _ = w.Write([]byte(time.Now().Format(time.RFC3339Nano)))
		return
	})
	srv = http3.New()
	buildErr := srv.Construct(transports.Options{
		Log: log,
		Config: transports.Config{
			Port:        18080,
			TLS:         transports.FixedTLSConfig(ssl.NewDefaultConfig(srvTLS, cliTLS, nil, nil)),
			Options:     nil,
			Middlewares: nil,
			Handlers:    nil,
		},
		Handler: handler,
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
		EnableDatagrams:      true,
		MaxRequestBodySize:   "",
		MaxRequestHeaderSize: "",
		AdditionalSettings:   nil,
		Quic:                 nil,
		Client:               nil,
		Alternative: &http3.AlternativeConfig{
			Name:    "fasthttp",
			Options: nil,
		},
	}
	options, encodeErr := json.Marshal(config)
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	srv := http3.NewWithAlternative(&fast.Transport{})
	handler := transports.HandlerFunc(func(w transports.ResponseWriter, r transports.Request) {
		fmt.Println("proto:", string(r.Proto()))
		w.SetStatus(200)
		_, _ = w.Write([]byte(time.Now().Format(time.RFC3339Nano)))
		return
	})
	buildErr := srv.Construct(transports.Options{
		Log: log,
		Config: transports.Config{
			Port:        18080,
			TLS:         transports.FixedTLSConfig(ssl.NewDefaultConfig(srvTLS, cliTLS, nil, nil)),
			Options:     options,
			Middlewares: nil,
			Handlers:    nil,
		},
		Handler: handler,
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
