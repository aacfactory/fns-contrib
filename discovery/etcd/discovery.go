package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/discovery"
	"github.com/rs/xid"
	client "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

const (
	namespace  = "_fns_"
	defaultTTL = 62 * time.Second
)

func Retriever(options ...discovery.Option) (d discovery.Discovery, err error) {
	if options == nil || len(options) == 0 {
		err = fmt.Errorf("retrieve etcd discovery failed, options is invalid")
		return
	}
	opt := &discovery.Options{}
	for _, option := range options {
		optErr := option(opt)
		if optErr != nil {
			err = optErr
			return
		}
	}
	connectConfig := ConnectConfig{}
	decodeErr := json.Unmarshal(opt.Config, &connectConfig)
	if decodeErr != nil {
		err = fmt.Errorf("retrieve etcd discovery failed, config is invalid")
		return
	}
	config := client.Config{
		Endpoints:   connectConfig.Endpoints,
		Username:    connectConfig.Username,
		Password:    connectConfig.Password,
		DialTimeout: connectConfig.DialTimeout,
	}
	if connectConfig.TLS.Enable {
		clientTLSConfig, tlsErr := connectConfig.TLS.Config()
		if tlsErr != nil {
			err = fmt.Errorf("retrieve etcd discovery failed, client tls in config is invalid")
			return
		}
		config.TLS = clientTLSConfig
	}

	ec, connErr := client.New(config)
	if connErr != nil {
		err = fmt.Errorf("retrieve etcd discovery failed for connetion, %v", connErr)
		return
	}

	grant, grantErr := ec.Grant(context.TODO(), connectConfig.GrantTTL.Milliseconds()/1000)
	if grantErr != nil {
		err = fmt.Errorf("retrieve etcd discovery failed for etcd grant failed")
		return
	}

	ed := &etcdDiscovery{
		ec:        ec,
		address:   opt.Address,
		clientTLS: opt.ClientTLS,
		leaseId:   grant.ID,
	}

	ed.keepalive()
	ed.watching()

	d = ed

	return
}

func WithConnectionConfig(config ConnectConfig) discovery.Option {
	return func(o *discovery.Options) (err error) {
		if config.GrantTTL <= time.Second {
			config.GrantTTL = defaultTTL
		}
		content, encodeErr := json.Marshal(config)
		if encodeErr != nil {
			err = fmt.Errorf("discovery create config option failed, config is invalied")
			return
		}
		o.Config = content
		return nil
	}
}

type ConnectConfig struct {
	Endpoints   []string            `json:"endpoints"`
	Username    string              `json:"username,omitempty"`
	Password    string              `json:"password,omitempty"`
	DialTimeout time.Duration       `json:"dialTimeout,omitempty"`
	GrantTTL    time.Duration       `json:"grantTtl,omitempty"`
	TLS         discovery.ClientTLS `json:"tls,omitempty"`
}

type etcdDiscovery struct {
	ec            *client.Client
	address       string
	clientTLS     discovery.ClientTLS
	leaseId       client.LeaseID
	registrations []discovery.Registration
}

func (d *etcdDiscovery) keyOfRegistration(registration discovery.Registration) (key string) {
	key = fmt.Sprintf("%s/fn/%s/%s", namespace, registration.Name, registration.Id)
	return
}

func (d *etcdDiscovery) Publish(name string) (registrationId string, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		err = fmt.Errorf("fns discovery publish fn failed, name is invailed")
		return
	}
	registration := discovery.Registration{
		Id:        xid.New().String(),
		Name:      name,
		Address:   d.address,
		ClientTLS: d.clientTLS,
	}
	registrationContent, toJsonErr := json.Marshal(registration)
	if toJsonErr != nil {
		err = fmt.Errorf("fns discovery publish fn failed for encode registration")
		return
	}
	key := d.keyOfRegistration(registration)

	_, putErr := d.ec.Put(context.TODO(), key, string(registrationContent), client.WithLease(d.leaseId))
	if putErr != nil {
		err = fmt.Errorf("fns discovery publish failed, etcd put failed")
		return
	}

	d.registrations = append(d.registrations, registration)

	registrationId = registration.Id

	return
}

func (d *etcdDiscovery) UnPublish(registrationId string) (err error) {

	return
}

func (d *etcdDiscovery) Get(name string) (registrations []discovery.Registration, err error) {
	return
}

func (d *etcdDiscovery) IsLocaled(name string) (ok bool) {
	return
}

func (d *etcdDiscovery) Close() {
	return
}

func (d *etcdDiscovery) keepalive() {
	return
}

func (d *etcdDiscovery) watching() {
	return
}
