package kubernetes

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	kb "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"strings"
	"sync"
	"time"
)

func newKube(namespace string, checkingTTL time.Duration) (k *Kube, err error) {
	config, configErr := rest.InClusterConfig()
	if configErr != nil {
		err = fmt.Errorf("get kubernetes cluster config failed, %v", configErr)
		return
	}
	client, clientErr := kb.NewForConfig(config)
	if clientErr != nil {
		err = fmt.Errorf("create clientset from kubernetes cluster config failed, %v", clientErr)
		return
	}


	k = &Kube{
		namespace:        namespace,
		mutex:            sync.RWMutex{},
		client:           client,
		localMap:         make(map[string]*fns.LocaledServiceProxy),
		serviceMap:       make(map[string]*fns.RemotedServiceProxyGroup),
		podsMap:          make(map[string]*fns.RemotedServiceProxyGroup),
		checkingTTL:      checkingTTL,
		checkingClosedCh: make(chan struct{}, 1),
	}

	initErr := k.init()
	if initErr != nil {
		err = fmt.Errorf("create clientset from kubernetes cluster config failed, %v", initErr)
		return
	}

	k.checking()

	return
}

type Kube struct {
	namespace  string
	mutex      sync.RWMutex
	client     *kb.Clientset
	localMap   map[string]*fns.LocaledServiceProxy

	serviceMap map[string]*fns.RemotedServiceProxyGroup
	podsMap    map[string]*fns.RemotedServiceProxyGroup

	checkingTTL      time.Duration
	checkingClosedCh chan struct{}
}

func (k *Kube) Publish(svc fns.Service) (err error) {

	// todo：remote 的 统一使用 md5 host address 做 id
	name := strings.TrimSpace(svc.Namespace())
	if name == "" {
		err = fmt.Errorf("fns Kubernetes Discovery Publish: namespace is invailed")
		return
	}
	k.mutex.Lock()
	k.localMap[name] = fns.NewLocaledServiceProxy(svc)
	k.mutex.Unlock()
	return
}

func (k *Kube) IsLocal(namespace string) (ok bool) {
	_, ok = k.localMap[namespace]
	return
}

// Proxy get service
func (k *Kube) Proxy(_ fns.Context, namespace string) (proxy fns.ServiceProxy, err errors.CodeError) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		err = errors.NotFound("fns Kubernetes Discovery Proxy: namespace is empty")
		return
	}
	// get from local
	localProxy, localed := k.localMap[namespace]
	if localed {
		proxy = localProxy
		return
	}

	// get from remote
	k.mutex.RLock()
	group, has := k.serviceMap[namespace]
	k.mutex.RUnlock()
	if !has {
		labelSelector := fmt.Sprintf("%s in (%s)", label, namespace)
		groupServices, groupServicesErr := getServices(k.client, k.namespace, labelSelector)
		if groupServicesErr != nil {
			err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Kubernetes Discovery Proxy: get %s service from kubernetes failed", namespace)).WithCause(groupServicesErr)
			return
		}

		if groupServices == nil && len(groupServices) == 0 {
			err = errors.NotFound(fmt.Sprintf("fns Kubernetes Discovery Proxy: %s was not found", namespace))
			return
		}

		for ns, _group := range groupServices {
			proxyGroup := fns.NewRemotedServiceProxyGroup(ns)
			for _, service := range _group {
				agent := fns.NewRemotedServiceProxy(service.Id, service.Name, service.Address)
				proxyGroup.AppendAgent(agent)
			}
			k.mutex.Lock()
			k.serviceMap[namespace] = proxyGroup
			k.mutex.Unlock()
			if ns == namespace {
				group = proxyGroup
			}
		}
	}
	proxy, err = group.Next()
	return
}

//ProxyByExact get pod
func (k *Kube) ProxyByExact(_ fns.Context, proxyId string) (proxy fns.ServiceProxy, err errors.CodeError) {
	proxyId = strings.TrimSpace(proxyId)
	if proxyId == "" {
		err = errors.NotFound("fns Kubernetes Discovery ProxyByExact: proxyId is empty")
		return
	}

	// local
	registration, hasRegistration := k.registrations[proxyId]
	if hasRegistration {
		localProxy, has := d.localMap[registration.Name]
		if !has {
			err = errors.New(555, "***WARNING***", "fns Etcd Service Discovery ProxyByExact: found in local but not exists")
			return
		}
		proxy = localProxy
		return
	}

	k.mutex.RLock()
	group, has := k.serviceMap[namespace]
	k.mutex.RUnlock()

	// get pod
	return
}

func (k *Kube) init() (err error) {
	groupServices, groupServicesErr := getServices(k.client, k.namespace, label)
	if groupServicesErr != nil {
		err = fmt.Errorf("fns Kubernetes Discovery init: %v", groupServicesErr)
		return
	}

	groupPods, groupPodsErr := getPods(k.client, k.namespace, label)
	if groupPodsErr != nil {
		err = fmt.Errorf("fns Kubernetes Discovery init: %v", groupPodsErr)
		return
	}

	if groupServices != nil && len(groupServices) > 0 {
		for namespace, group := range groupServices {
			proxyGroup := fns.NewRemotedServiceProxyGroup(namespace)
			for _, service := range group {
				proxy := fns.NewRemotedServiceProxy(service.Id, service.Name, service.Address)
				proxyGroup.AppendAgent(proxy)
			}
			k.serviceMap[namespace] = proxyGroup
		}
	}

	if groupPods != nil && len(groupPods) > 0 {
		for namespace, group := range groupPods {
			proxyGroup := fns.NewRemotedServiceProxyGroup(namespace)
			for _, pod := range group {
				proxy := fns.NewRemotedServiceProxy(pod.Id, pod.Name, pod.Address)
				proxyGroup.AppendAgent(proxy)
			}
			k.podsMap[namespace] = proxyGroup
		}
	}

	return
}

func (k *Kube) Close() {

	close(k.checkingClosedCh)

	for key := range k.localMap {
		k.discovered.remove(key)
	}

	k.discovered.close()

	return
}

func (k *Kube) checking() {
	go func(k *Kube) {
		timer := time.NewTimer(k.checkingTTL)
		for {
			stopped := false
			select {
			case <-k.checkingClosedCh:
				stopped = true
				return

			case <-timer.C:
				k.discovered.check()
			}
			if stopped {
				break
			}
		}
		timer.Stop()
	}(k)
}
