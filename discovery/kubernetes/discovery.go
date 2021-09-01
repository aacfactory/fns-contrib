package kubernetes

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	kb "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"reflect"
	"strings"
)

func newKube(namespace string, httpClientPoolSize int) (k *Kube, err error) {
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

	timeout := int64(3)
	w, watchErr := client.CoreV1().Services(k.kubeNS).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector:       label,
		Watch:               true,
		AllowWatchBookmarks: false,
		TimeoutSeconds:      &timeout,
	})

	if watchErr != nil {
		err = fmt.Errorf("create clientset from kubernetes cluster config failed, %v", watchErr)
		return
	}

	k = &Kube{
		AbstractServiceDiscovery: fns.NewAbstractServiceDiscovery(httpClientPoolSize),
		kubeNS:                   namespace,
		client:                   client,
		watchingClosedCh:         make(chan struct{}, 1),
		watcher:                  w,
	}

	initErr := k.init()
	if initErr != nil {
		err = fmt.Errorf("create clientset from kubernetes cluster config failed, %v", initErr)
		return
	}

	k.watching()

	return
}

type Kube struct {
	fns.AbstractServiceDiscovery
	kubeNS           string
	client           *kb.Clientset
	watchingClosedCh chan struct{}
	watcher          watch.Interface
}

func (k *Kube) Publish(svc fns.Service) (err error) {
	err = k.Local.Publish(svc)
	return
}

func (k *Kube) init() (err error) {
	groupServices, groupServicesErr := getServices(k.client, k.kubeNS, label)
	if groupServicesErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery init: %v", groupServicesErr)
		return
	}

	if groupServices != nil && len(groupServices) > 0 {
		for _, group := range groupServices {
			for _, service := range group {
				k.Manager.Append(service)
			}
		}
	}

	return
}

func (k *Kube) Close() {
	close(k.watchingClosedCh)
	k.watcher.Stop()
	k.AbstractServiceDiscovery.Close()
	return
}

func (k *Kube) watching() {
	go func(k *Kube) {
		for {
			stopped := false
			select {
			case <-k.watchingClosedCh:
				stopped = true
				break
			case result, ok := <-k.watcher.ResultChan():
				if !ok {
					break
				}
				if result.Type != watch.Added && result.Type != watch.Deleted && result.Type != watch.Error && result.Type != watch.Modified {
					break
				}
				svc, isService := result.Object.(*coreV1.Service)
				if !isService {
					panic(fmt.Sprintf("fns ServiceDiscovery init: kubernetes watch fns service failed, got not kube service type watch result, %v", reflect.TypeOf(result.Object)))
				}

				if svc.Spec.Type != coreV1.ServiceTypeClusterIP {
					break
				}
				fnsLabel, hasLabel := svc.Labels[label]
				if !hasLabel {
					break
				}
				servicePort := 0
				for _, port := range svc.Spec.Ports {
					if port.Name == label {
						servicePort = int(port.Port)
						break
					}
				}
				if servicePort == 0 {
					break
				}
				serviceId := string(svc.UID)
				reversion := svc.CreationTimestamp.Unix()
				serviceIp := svc.Spec.ClusterIP

				namespaces := strings.Split(fnsLabel, ",")
				for _, namespace := range namespaces {
					registration := fns.Registration{
						Id:        serviceId,
						Namespace: strings.TrimSpace(namespace),
						Address:   fmt.Sprintf("%s:%d", serviceIp, servicePort),
						Reversion: reversion,
					}
					switch result.Type {
					case watch.Added, watch.Modified:
						k.Manager.Append(registration)
					case watch.Error, watch.Deleted:
						k.Manager.Remove(registration)
					default:

					}
				}

			}
			if stopped {
				break
			}
		}
	}(k)
}
