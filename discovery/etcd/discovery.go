package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	client "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
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
	mutex             sync.RWMutex
	ec                *client.Client
	address           string
	grantTTL          time.Duration
	leaseId           client.LeaseID
	localMap          map[string]*fns.LocaledServiceProxy
	remoteMap         map[string]*fns.RemotedServiceProxyGroup
	registrations     map[string]Registration
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

	proxy := fns.NewLocaledServiceProxy(svc)

	registration := Registration{
		Id:      proxy.Id(),
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

	d.localMap[name] = proxy

	d.registrations[registration.Id] = registration

	return
}

func (d *etcdDiscovery) IsLocal(namespace string) (ok bool) {
	_, ok = d.localMap[namespace]
	return
}

func (d *etcdDiscovery) Proxy(_ fns.Context, namespace string) (proxy fns.ServiceProxy, err errors.CodeError) {
	name := strings.TrimSpace(namespace)
	if name == "" {
		err = errors.NotFound("fns Etcd Service Discovery Proxy: namespace is empty")
		return
	}

	// get from local
	proxy0, localed := d.localMap[name]
	if localed {
		proxy = proxy0
		return
	}

	// get from remote
	d.mutex.RLock()
	group, has := d.remoteMap[name]
	d.mutex.RUnlock()
	if !has {
		result, getErr := d.ec.Get(context.TODO(), d.keyOfService(name), client.WithPrefix())
		if getErr != nil {
			err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Etcd Service Discovery ProxyByExact: get %s from etcd failed", d.keyOfService(name))).WithCause(getErr)
			return
		}
		if result.Count == 0 {
			err = errors.NotFound(fmt.Sprintf("fns Etcd Service Discovery Proxy: %s was not found", name))
			return
		}
		group = fns.NewRemotedServiceProxyGroup(name)
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
			agent := fns.NewRemotedServiceProxy(registration.Id, registration.Name, registration.Address)
			group.AppendAgent(agent)

		}
		d.mutex.Lock()
		d.remoteMap[name] = group
		d.mutex.Unlock()
	}

	proxy, err = group.Next()

	return
}

func (d *etcdDiscovery) ProxyByExact(_ fns.Context, proxyId string) (proxy fns.ServiceProxy, err errors.CodeError) {

	// local
	registration, hasRegistration := d.registrations[proxyId]
	if hasRegistration {
		localProxy, has := d.localMap[registration.Name]
		if !has {
			err = errors.New(555, "***WARNING***", "fns Etcd Service Discovery ProxyByExact: found in local but not exists")
			return
		}
		proxy = localProxy
		return
	}

	// remotes
	d.mutex.RLock()
	for _, group := range d.remoteMap {
		agent, agentErr := group.GetAgent(proxyId)
		if agentErr == nil {
			proxy = agent
			d.mutex.RUnlock()
			return
		}
	}
	d.mutex.RUnlock()

	result, getErr := d.ec.Get(context.TODO(), prefix, client.WithPrefix())
	if getErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Etcd Service Discovery ProxyByExact: get %s from etcd failed", prefix)).WithCause(getErr)
		return
	}
	if result.Count == 0 {
		err = errors.New(555, "***WARNING***", "fns Etcd Service Discovery ProxyByExact: not found")
		return
	}

	serviceNamespace := ""
	registrationsMap := make(map[string][]Registration)
	for _, kv := range result.Kvs {
		key := string(kv.Key)
		keyItems := strings.Split(key, "/")
		if len(keyItems) != 4 {
			continue
		}
		name := keyItems[2]
		id := keyItems[3]
		if serviceNamespace == "" && id == proxyId {
			serviceNamespace = name
		}

		registrations, hasRegistrations := registrationsMap[name]
		if !hasRegistrations {
			registrations = make([]Registration, 0, 1)
		}
		_registration := Registration{}
		decodeErr := json.Unmarshal(kv.Value, &_registration)
		if decodeErr != nil {
			continue
		}
		registrations = append(registrations, _registration)
		registrationsMap[name] = registrations
	}

	if len(registrationsMap) == 0 || serviceNamespace == "" {
		err = errors.New(555, "***WARNING***", "fns Etcd Service Discovery ProxyByExact: not found")
		return
	}
	registrations := registrationsMap[serviceNamespace]
	group := fns.NewRemotedServiceProxyGroup(serviceNamespace)
	for _, _registration := range registrations {
		agent := fns.NewRemotedServiceProxy(_registration.Id, _registration.Name, _registration.Address)
		group.AppendAgent(agent)
	}

	agent, agentErr := group.GetAgent(proxyId)
	if agentErr != nil {
		err = errors.New(555, "***WARNING***", "fns Etcd Service Discovery ProxyByExact: not found")
		return
	}

	proxy = agent

	d.mutex.Lock()
	d.remoteMap[serviceNamespace] = group
	d.mutex.Unlock()

	return
}

func (d *etcdDiscovery) init() (err error) {

	result, getErr := d.ec.Get(context.TODO(), prefix, client.WithPrefix())
	if getErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Etcd Service Discovery init: get %s from etcd failed", prefix)).WithCause(getErr)
		return
	}
	if result.Count == 0 {
		return
	}

	registrationsMap := make(map[string][]Registration)
	for _, kv := range result.Kvs {
		key := string(kv.Key)
		keyItems := strings.Split(key, "/")
		if len(keyItems) != 4 {
			continue
		}
		name := keyItems[2]

		registrations, hasRegistrations := registrationsMap[name]
		if !hasRegistrations {
			registrations = make([]Registration, 0, 1)
		}
		_registration := Registration{}
		decodeErr := json.Unmarshal(kv.Value, &_registration)
		if decodeErr != nil {
			continue
		}
		registrations = append(registrations, _registration)
		registrationsMap[name] = registrations
	}

	for name, registrations := range registrationsMap {
		group := fns.NewRemotedServiceProxyGroup(name)
		for _, registration := range registrations {
			agent := fns.NewRemotedServiceProxy(registration.Id, registration.Name, registration.Address)
			group.AppendAgent(agent)
		}
		d.remoteMap[name] = group
	}

	return
}

func (d *etcdDiscovery) Close() {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	close(d.watchingClosedCh)

	for _, registration := range d.registrations {
		key := d.keyOfRegistration(registration)
		_, _ = d.ec.Delete(context.TODO(), key)
	}

	for key := range d.registrations {
		delete(d.registrations, key)
	}

	_ = d.ec.Close()

	for _, group := range d.remoteMap {
		group.Close()
	}

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
					id := keyItems[3]
					name := keyItems[2]
					if _, has := d.registrations[id]; has {
						continue
					}

					if event.Type == 0 {
						// save
						registration := Registration{}
						decodeErr := json.Unmarshal(event.Kv.Value, &registration)
						if decodeErr != nil {
							continue
						}
						d.mutex.RLock()
						group, has := d.remoteMap[registration.Name]
						d.mutex.RUnlock()
						if !has {
							group = fns.NewRemotedServiceProxyGroup(registration.Name)
							d.mutex.Lock()
							d.remoteMap[registration.Name] = group
							d.mutex.Unlock()
						}
						agent := fns.NewRemotedServiceProxy(registration.Id, registration.Name, registration.Address)
						group.AppendAgent(agent)
					} else {
						// remove
						d.mutex.RLock()
						group, has := d.remoteMap[name]
						d.mutex.RUnlock()
						if has {
							group.RemoveAgent(id)
							if group.AgentNum() == 0 {
								d.mutex.Lock()
								delete(d.remoteMap, name)
								d.mutex.Unlock()
							}
						}
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
