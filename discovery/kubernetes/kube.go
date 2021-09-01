package kubernetes

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kb "k8s.io/client-go/kubernetes"
	"strings"
)

// getServices [ns][serviceId]fns.Registration
func getServices(client *kb.Clientset, ns string, labelSelector string) (groupServices map[string]map[string]fns.Registration, err error) {
	si := client.CoreV1().Services(ns)
	if si == nil {
		err = fmt.Errorf("fns ServiceDiscovery: get %s kube service failed, namespace was not found in kubernetes", ns)
		return
	}
	timeout := int64(3)
	list, listErr := si.List(context.TODO(), metav1.ListOptions{
		TypeMeta:       metav1.TypeMeta{},
		LabelSelector:  labelSelector,
		TimeoutSeconds: &timeout,
		Limit:          0,
	})
	if listErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery: get %s kube service failed, %v", ns, listErr)
		return
	}
	if list == nil || list.Items == nil || len(list.Items) == 0 {
		err = fmt.Errorf("fns ServiceDiscovery: get %s kube service failed, got empty services", ns)
		return
	}
	groupServices = make(map[string]map[string]fns.Registration)
	for _, item := range list.Items {

		if item.Spec.Type != coreV1.ServiceTypeClusterIP {
			continue
		}

		fnsLabel, hasLabel := item.Labels[label]
		if !hasLabel {
			continue
		}

		servicePort := 0
		for _, port := range item.Spec.Ports {
			if port.Name == label {
				servicePort = int(port.Port)
				break
			}
		}
		if servicePort == 0 {
			err = fmt.Errorf("fns ServiceDiscovery: get %s kube service failed, got fns service but no fns port", ns)
			return
		}

		serviceId := string(item.UID)
		reversion := item.CreationTimestamp.Unix()
		serviceIp := item.Spec.ClusterIP

		namespaces := strings.Split(fnsLabel, ",")
		for _, namespace := range namespaces {
			namespace = strings.TrimSpace(namespace)

			group, hasGroup := groupServices[namespace]
			if !hasGroup {
				group = make(map[string]fns.Registration)
			}

			_, hasService := group[serviceId]
			if hasService {
				continue
			}

			group[serviceId] = fns.Registration{
				Id:        serviceId,
				Namespace: namespace,
				Address:   fmt.Sprintf("%s:%d", serviceIp, servicePort),
				Reversion: reversion,
			}
		}
	}
	return
}
