package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/rs/xid"
	client "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

const (
	prefix     = "_fns_/svc"
	defaultTTL = 62
)

type Registration struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type etcdDiscovery struct {
	ec                *client.Client
	address           string
	grantTTL          time.Duration
	leaseId           client.LeaseID
	serviceMap        map[string]fns.Service
	registrations     map[string]Registration
	discovered        *proxyCache
	keepAliveClosedCh chan struct{}
	watchingClosedCh  chan struct{}
}

func (d *etcdDiscovery) keyOfRegistration(registration Registration) (key string) {
	key = fmt.Sprintf("%s/%s/%s", prefix, registration.Name, registration.Id)
	return
}

func (d *etcdDiscovery) keyOfService(namespace string) (key string) {
	key = fmt.Sprintf("%s/%s", prefix, namespace)
	return
}

func (d *etcdDiscovery) Publish(svc fns.Service) (err error) {
	name := strings.TrimSpace(svc.Namespace())
	if name == "" {
		err = fmt.Errorf("fns Etcd Service Discovery Publish: namespace is invailed")
		return
	}
	registration := Registration{
		Id:      xid.New().String(),
		Name:    name,
		Address: d.address,
	}
	registrationContent, toJsonErr := json.Marshal(registration)
	if toJsonErr != nil {
		err = fmt.Errorf("fns Etcd Service Discovery Publish: encode registration failed, %v", toJsonErr)
		return
	}
	key := d.keyOfRegistration(registration)

	_, putErr := d.ec.Put(context.TODO(), key, string(registrationContent), client.WithLease(d.leaseId))
	if putErr != nil {
		err = fmt.Errorf("fns Etcd Service Discovery Publish: save registration failed, %v", putErr)
		return
	}

	d.serviceMap[name] = svc

	d.registrations[name] = registration

	return
}

func (d *etcdDiscovery) IsLocal(namespace string) (ok bool) {
	_, ok = d.serviceMap[namespace]
	return
}

func (d *etcdDiscovery) Proxy(namespace string) (proxy fns.ServiceProxy, err errors.CodeError) {
	name := strings.TrimSpace(namespace)
	if name == "" {
		err = errors.NotFound("fns Etcd Service Discovery Proxy: namespace is empty")
		return
	}
	// get from local
	service, localed := d.serviceMap[name]
	if localed {
		proxy = &fns.LocaledServiceProxy{
			Service: service,
		}
		return
	}

	// get from remote
	remoted, has := d.discovered.get(name)
	if !has {

		result, getErr := d.ec.Get(context.TODO(), d.keyOfService(name), client.WithPrefix())
		if getErr != nil {
			err = errors.New(555, "***WARNING***", fmt.Sprintf("get %s from etcd failed", d.keyOfService(name))).WithCause(getErr)
			return
		}
		if result.Count == 0 {
			err = errors.NotFound(fmt.Sprintf("fns Etcd Service Discovery Proxy: %s was not found", name))
			return
		}

		for _, kv := range result.Kvs {
			key := string(kv.Key)
			keyItems := strings.Split(key, "/")
			if len(keyItems) != 4 {
				continue
			}
			registration := Registration{}
			decodeErr := json.Unmarshal(kv.Value, &registration)
			if decodeErr != nil {
				continue
			}
			d.discovered.put(registration)
		}

		proxy0, has0 := d.discovered.get(name)
		if !has0 {
			err = errors.NotFound(fmt.Sprintf("fns Etcd Service Discovery Proxy: %s was not found", name))
			return
		}
		proxy = proxy0
		return
	} else {
		proxy = remoted
	}

	return
}

func (d *etcdDiscovery) Close() {

	close(d.watchingClosedCh)

	for _, registration := range d.registrations {
		key := d.keyOfRegistration(registration)
		_, _ = d.ec.Delete(context.TODO(), key)
	}

	for key := range d.registrations {
		delete(d.registrations, key)
	}

	_ = d.ec.Close()

	d.discovered.close()

	return
}

func (d *etcdDiscovery) keepalive() {
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
	}(d)
	return
}

func (d *etcdDiscovery) watching() {

	go func(d *etcdDiscovery) {
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
					id := keyItems[3]
					name := keyItems[2]
					if _, has := d.registrations[name]; has {
						continue
					}

					// todo: remote proxy cache update
					if event.Type == 0 {
						// save
						registration := Registration{}
						decodeErr := json.Unmarshal(event.Kv.Value, &registration)
						if decodeErr != nil {
							continue
						}
						d.discovered.put(registration)
					} else {
						// remove
						registration := Registration{
							Id:      id,
							Name:    name,
							Address: "",
						}
						d.discovered.remove(registration)
					}
				}
			case <-d.watchingClosedCh:
				stopped = true
				break
			}
		}
		cancel()
	}(d)
	return
}
