package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	client "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"
)

const (
	prefix     = "_fns_/svc"
	defaultTTL = 62
)

func newEtcdDiscovery(option fns.ServiceDiscoveryOption) (discovery *etcdDiscovery, err error) {
	config := Config{}
	configErr := option.Config.As(&config)
	if configErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: read config failed, %v", configErr)
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
			err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but certFilePath is empty")
			return
		}
		keyFilePath := strings.TrimSpace(config.KeyFilePath)
		if keyFilePath == "" {
			err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but keyFilePath is empty")
			return
		}
		var absErr error
		certFilePath, absErr = filepath.Abs(certFilePath)
		if absErr != nil {
			err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but get absolute representation of certFilePath failed, %v", absErr)
			return
		}
		keyFilePath, absErr = filepath.Abs(keyFilePath)
		if absErr != nil {
			err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but get absolute representation of keyFilePath failed, %v", absErr)
			return
		}
		certificate, certificateErr := tls.LoadX509KeyPair(certFilePath, keyFilePath)
		if certificateErr != nil {
			err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but load x509 key pair failed, %v", certificateErr)
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
				err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but get absolute representation of caFilePath failed, %v", absErr)
				return
			}
			caContent, caReadErr := ioutil.ReadFile(caFilePath)
			if caReadErr != nil {
				err = fmt.Errorf("fns Etcd Service Discovery Retriever: ssl is enabled but read caFilePath content failed, %v", caReadErr)
				return
			}
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caContent) {
				err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: ssl is enabled but append ca into cert pool failed")
				return
			}
			ssl.RootCAs = caPool
		}
		etcdConfig.TLS = ssl
	}

	ec, connErr := client.New(etcdConfig)
	if connErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: connect to etcd failed, %v", connErr)
		return
	}

	grantTTL := config.GrantTTLSecond
	if grantTTL <= 1 {
		grantTTL = defaultTTL
	}

	grant, grantErr := ec.Grant(context.TODO(), int64(grantTTL))
	if grantErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery EtcdRetriever: grant failed, %v", grantErr)
		return
	}

	ed := &etcdDiscovery{
		AbstractServiceDiscovery: fns.NewAbstractServiceDiscovery(option.HttpClientPoolSize),
		ec:                       ec,
		address:                  option.Address,
		grantTTL:                 time.Duration(grantTTL) * time.Second,
		leaseId:                  grant.ID,
		keepAliveClosedCh:        make(chan struct{}, 1),
		watchingClosedCh:         make(chan struct{}, 1),
	}

	initErr := ed.init()
	if initErr != nil {
		err = initErr
		return
	}

	ed.keepalive()
	ed.watching()

	discovery = ed

	return
}

type etcdDiscovery struct {
	fns.AbstractServiceDiscovery
	ec                *client.Client
	address           string
	grantTTL          time.Duration
	leaseId           client.LeaseID
	keepAliveClosedCh chan struct{}
	watchingClosedCh  chan struct{}
}

func (discovery *etcdDiscovery) keyOfRegistration(registration fns.Registration) (key string) {
	key = fmt.Sprintf("%s/%s/%s", prefix, registration.Namespace, registration.Id)
	return
}

func (discovery *etcdDiscovery) keyOfService(namespace string) (key string) {
	key = fmt.Sprintf("%s/%s", prefix, namespace)
	return
}

func (discovery *etcdDiscovery) Publish(svc fns.Service) (err error) {

	ns := strings.TrimSpace(svc.Namespace())
	if ns == "" {
		err = fmt.Errorf("fns ServiceDiscovery Publish: namespace is invailed")
		return
	}

	registration := fns.Registration{
		Id:        fns.UID(),
		Namespace: ns,
		Address:   discovery.address,
	}
	registrationContent, toJsonErr := json.Marshal(registration)
	if toJsonErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery Publish: encode registration failed, %v", toJsonErr)
		return
	}
	key := discovery.keyOfRegistration(registration)

	_, putErr := discovery.ec.Put(context.TODO(), key, string(registrationContent), client.WithLease(discovery.leaseId))
	if putErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery Publish: save registration failed, %v", putErr)
		return
	}

	localErr := discovery.Local.Publish(svc)
	if localErr != nil {
		_, _ = discovery.ec.Delete(context.TODO(), key)
		err = localErr
		return
	}

	discovery.Manager.Append(registration)

	return
}

func (discovery *etcdDiscovery) init() (err error) {

	result, getErr := discovery.ec.Get(context.TODO(), prefix, client.WithPrefix())
	if getErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns ServiceDiscovery init: get %s from etcd failed", prefix)).WithCause(getErr)
		return
	}
	if result.Count == 0 {
		return
	}

	for _, kv := range result.Kvs {
		key := string(kv.Key)
		keyItems := strings.Split(key, "/")
		if len(keyItems) != 4 {
			continue
		}

		registration := fns.Registration{}
		decodeErr := json.Unmarshal(kv.Value, &registration)
		if decodeErr != nil {
			continue
		}
		discovery.Manager.Append(registration)
	}

	return
}

func (discovery *etcdDiscovery) Close() {

	close(discovery.watchingClosedCh)

	for _, registration := range discovery.Manager.Registrations() {
		key := discovery.keyOfRegistration(registration)
		_, _ = discovery.ec.Delete(context.TODO(), key)
	}

	_ = discovery.ec.Close()

	discovery.AbstractServiceDiscovery.Close()

	return
}

func (discovery *etcdDiscovery) keepalive() {
	go func(d *etcdDiscovery) {
		ttl := d.grantTTL
		timeout := ttl / 3
		stopped := false
		for {
			if stopped {
				break
			}
			select {
			case <-time.After(timeout):
				_, keepAliveErr := d.ec.KeepAliveOnce(context.TODO(), d.leaseId)
				if keepAliveErr != nil {
					for i := 0; i < 5; i++ {
						_, keepAliveErr = d.ec.KeepAliveOnce(context.TODO(), d.leaseId)
						if keepAliveErr == nil {
							break
						}
						time.Sleep(1 * time.Second)
					}
				}
			case <-d.keepAliveClosedCh:
				stopped = true
				break
			}
		}
	}(discovery)
	return
}

func (discovery *etcdDiscovery) watching() {
	go func(d *etcdDiscovery) {
		time.Sleep(3 * time.Second)
		ctx, cancel := context.WithCancel(context.TODO())
		watchCh := d.ec.Watch(ctx, prefix, client.WithPrefix())
		stopped := false
		for {
			if stopped {
				break
			}
			select {
			case registrationEvent, ok := <-watchCh:
				if !ok {
					stopped = true
					break
				}
				events := registrationEvent.Events
				for _, event := range events {
					key := string(event.Kv.Key)

					keyItems := strings.Split(key, "/")
					if len(keyItems) != 4 {
						continue
					}

					if event.Type == 0 {
						// save
						registration := fns.Registration{}
						decodeErr := json.Unmarshal(event.Kv.Value, &registration)
						if decodeErr != nil {
							continue
						}
						registration.Reversion = event.Kv.ModRevision
						d.Manager.Append(registration)

					} else {
						// remove
						id := keyItems[3]
						ns := keyItems[2]

						d.Manager.Remove(fns.Registration{
							Id:        id,
							Namespace: ns,
							Address:   "",
							Reversion: 0,
						})
					}
				}
			case <-d.watchingClosedCh:
				stopped = true
				break
			}
		}
		cancel()
	}(discovery)
	return
}
