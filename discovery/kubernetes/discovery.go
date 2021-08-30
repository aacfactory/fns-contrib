package kubernetes

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kb "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"strings"
	"time"
)

type Registration struct {
	Name    string
	Address string
}

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

	cache, createCacheErr := newProxyCache()
	if createCacheErr != nil {
		err = fmt.Errorf("create cache failed, %v", createCacheErr)
		return
	}

	k = &Kube{
		namespace:        namespace,
		client:           client,
		serviceMap:       make(map[string]fns.Service),
		discovered:       cache,
		checkingTTL:      checkingTTL,
		checkingClosedCh: make(chan struct{}, 1),
	}

	registrations, initErr := k.getAllByLabel()
	if initErr != nil {
		err = fmt.Errorf("create cache failed, %v", initErr)
		return
	}

	for _, registration := range registrations {
		k.discovered.put(registration)
	}

	k.checking()

	return
}

type Kube struct {
	namespace        string
	client           *kb.Clientset
	serviceMap       map[string]fns.Service
	discovered       *proxyCache
	checkingTTL      time.Duration
	checkingClosedCh chan struct{}
}

func (k *Kube) Publish(svc fns.Service) (err error) {
	name := strings.TrimSpace(svc.Namespace())
	if name == "" {
		err = fmt.Errorf("fns Kubernetes Discovery Publish: namespace is invailed")
		return
	}
	k.serviceMap[name] = svc
	return
}

func (k *Kube) IsLocal(namespace string) (ok bool) {
	_, ok = k.serviceMap[namespace]
	return
}

func (k *Kube) Proxy(namespace string) (proxy fns.ServiceProxy, err errors.CodeError) {
	name := strings.TrimSpace(namespace)
	if name == "" {
		err = errors.NotFound("fns Kubernetes Discovery Proxy: namespace is empty")
		return
	}
	// get from local
	service, localed := k.serviceMap[name]
	if localed {
		proxy = &fns.LocaledServiceProxy{
			Service: service,
		}
		return
	}

	// get from remote
	remoted, has := k.discovered.get(name)
	if !has {

		registrations, fetchErr := k.getAllByLabel()

		if fetchErr != nil {
			err = errors.New(555, "***WARNING***", fmt.Sprintf("get %s from kubernetes failed", name)).WithCause(fetchErr)
			return
		}

		registration, got := registrations[name]
		if !got {
			err = errors.NotFound(fmt.Sprintf("fns Kubernetes Discovery Proxy: %s was not found", name))
			return
		}

		proxy = k.discovered.put(registration)

		return
	} else {
		proxy = remoted
	}
	return
}

func (k *Kube) getAllByLabel() (registrations map[string]Registration, err error) {
	si := k.client.CoreV1().Services(k.namespace)
	if si == nil {
		err = fmt.Errorf("fns Kubernetes: get %s kube service failed, namespace was not found in kubernetes", k.namespace)
		return
	}
	timeout := int64(5)
	list, listErr := si.List(context.TODO(), metav1.ListOptions{
		TypeMeta:       metav1.TypeMeta{},
		LabelSelector:  label,
		TimeoutSeconds: &timeout,
		Limit:          0,
	})
	if listErr != nil {
		err = fmt.Errorf("fns Kubernetes: get %s kube service failed, %v", k.namespace, listErr)
		return
	}

	if list == nil || list.Items == nil || len(list.Items) == 0 {
		err = fmt.Errorf("fns Kubernetes: get %s kube service failed, got empty services", k.namespace)
		return
	}

	registrations = make(map[string]Registration)

	for _, item := range list.Items {
		address := ""
		if item.Spec.Type == coreV1.ServiceTypeClusterIP {
			address = fmt.Sprintf("%s:%d", item.Spec.ClusterIP, item.Spec.Ports[0].Port)
		} else if item.Spec.Type == coreV1.ServiceTypeNodePort {
			address = fmt.Sprintf("%s:%d", item.Spec.ClusterIP, item.Spec.Ports[0].NodePort)
		} else if item.Spec.Type == coreV1.ServiceTypeLoadBalancer {
			address = fmt.Sprintf("%s:%d", item.Spec.LoadBalancerIP, item.Spec.Ports[0].Port)
		} else if item.Spec.Type == coreV1.ServiceTypeExternalName {
			address = fmt.Sprintf("%s:%d", item.Spec.ExternalName, item.Spec.Ports[0].Port)
		}
		ns := strings.TrimSpace(item.Labels[label])
		namespaces := strings.Split(ns, ",")
		for _, namespace := range namespaces {
			namespace = strings.TrimSpace(namespace)
			registrations[namespace] = Registration{
				Name:    namespace,
				Address: address,
			}
		}
	}

	return
}

func (k *Kube) Close() {

	close(k.checkingClosedCh)

	for key := range k.serviceMap {
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
