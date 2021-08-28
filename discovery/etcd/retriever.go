package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/aacfactory/fns"
	client "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"
)

const (
	kind = "etcd"
)

func init() {
	fns.RegisterServiceDiscoveryRetriever(kind, Retriever)
}

func Retriever(option fns.ServiceDiscoveryOption) (discovery fns.ServiceDiscovery, err error) {
	config := Config{}
	configErr := option.Config.As(&config)
	if configErr != nil {
		err = fmt.Errorf("fns Etcd Service Discovery Retriever: read config failed, %v", configErr)
		return
	}

	dialTimeoutSecond := config.DialTimeoutSecond
	if dialTimeoutSecond <= 1 {
		dialTimeoutSecond = 10
	}
	dialTimeout := time.Duration(dialTimeoutSecond) * time.Second
	etcdConfig := client.Config{
		Endpoints:   config.Endpoints,
		Username:    config.Username,
		Password:    config.Password,
		DialTimeout: dialTimeout,
	}

	if config.SSL {
		certFilePath := strings.TrimSpace(config.CertFilePath)
		if certFilePath == "" {
			err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but certFilePath is empty")
			return
		}
		keyFilePath := strings.TrimSpace(config.KeyFilePath)
		if keyFilePath == "" {
			err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but keyFilePath is empty")
			return
		}
		var absErr error
		certFilePath, absErr = filepath.Abs(certFilePath)
		if absErr != nil {
			err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but get absolute representation of certFilePath failed, %v", absErr)
			return
		}
		keyFilePath, absErr = filepath.Abs(keyFilePath)
		if absErr != nil {
			err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but get absolute representation of keyFilePath failed, %v", absErr)
			return
		}
		certificate, certificateErr := tls.LoadX509KeyPair(certFilePath, keyFilePath)
		if certificateErr != nil {
			err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but load x509 key pair failed, %v", certificateErr)
			return
		}

		ssl := &tls.Config{
			Certificates:       []tls.Certificate{certificate},
			InsecureSkipVerify: config.InsecureSkipVerify,
		}

		caFilePath := strings.TrimSpace(config.CaFilePath)
		if caFilePath != "" {
			caFilePath, absErr = filepath.Abs(caFilePath)
			if absErr != nil {
				err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but get absolute representation of caFilePath failed, %v", absErr)
				return
			}
			caContent, caReadErr := ioutil.ReadFile(caFilePath)
			if caReadErr != nil {
				err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but read caFilePath content failed, %v", caReadErr)
				return
			}
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caContent) {
				err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but append ca into cert pool failed")
				return
			}
			ssl.RootCAs = caPool
		}
		etcdConfig.TLS = ssl
	}

	ec, connErr := client.New(etcdConfig)
	if connErr != nil {
		err = fmt.Errorf("fns Etcd Service Discovery Retriever: connect to etcd failed, %v", connErr)
		return
	}

	grantTTL := config.GrantTTLSecond
	if grantTTL <= 1 {
		grantTTL = defaultTTL
	}

	grant, grantErr := ec.Grant(context.TODO(), int64(grantTTL))
	if grantErr != nil {
		err = fmt.Errorf("fns Etcd Service Discovery Retriever: grant failed, %v", grantErr)
		return
	}

	cache, createCacheErr := newProxyCache()
	if createCacheErr != nil {
		err = fmt.Errorf("fns Etcd Service Discovery Retriever: create cache failed, %v", createCacheErr)
		return
	}

	ed := &etcdDiscovery{
		ec:                ec,
		address:           option.Address,
		grantTTL:          time.Duration(grantTTL) * time.Second,
		leaseId:           grant.ID,
		registrations:     make(map[string]Registration),
		discovered:        cache,
		keepAliveClosedCh: make(chan struct{}, 1),
		watchingClosedCh:  make(chan struct{}, 1),
		serviceMap:        make(map[string]fns.Service),
	}

	ed.keepalive()
	ed.watching()

	discovery = ed

	return
}
