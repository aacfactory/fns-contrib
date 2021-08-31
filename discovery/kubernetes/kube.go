package kubernetes

import (
	"context"
	"fmt"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kb "k8s.io/client-go/kubernetes"
	"strings"
)

type Service struct {
	Id      string // UID
	Name    string
	Address string
}

type Pod struct {
	Id      string // UID
	Name    string
	Address string
}

// getServices [ns][serviceId]Service
func getServices(client *kb.Clientset, ns string, labelSelector string) (groupServices map[string]map[string]Service, err error) {
	si := client.CoreV1().Services(ns)
	if si == nil {
		err = fmt.Errorf("fns Kubernetes: get %s kube service failed, namespace was not found in kubernetes", ns)
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
		err = fmt.Errorf("fns Kubernetes: get %s kube service failed, %v", ns, listErr)
		return
	}
	if list == nil || list.Items == nil || len(list.Items) == 0 {
		err = fmt.Errorf("fns Kubernetes: get %s kube service failed, got empty services", ns)
		return
	}
	groupServices = make(map[string]map[string]Service)
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
			err = fmt.Errorf("fns Kubernetes: get %s kube service failed, got fns service but no fns port", ns)
			return
		}

		serviceId := string(item.UID)

		serviceIp := item.Spec.ClusterIP

		namespaces := strings.Split(fnsLabel, ",")
		for _, namespace := range namespaces {
			namespace = strings.TrimSpace(namespace)

			group, hasGroup := groupServices[namespace]
			if !hasGroup {
				group = make(map[string]Service)
			}

			_, hasService := group[serviceId]
			if hasService {
				continue
			}

			group[serviceId] = Service{
				Id:      serviceId,
				Name:    namespace,
				Address: fmt.Sprintf("%s:%d", serviceIp, servicePort),
			}
		}
	}
	return
}

// getPods
// [ns][podId]Pod
func getPods(client *kb.Clientset, ns string, labelSelector string) (groupPods map[string]map[string]Pod, err error) {
	pi := client.CoreV1().Pods(ns)
	if pi == nil {
		err = fmt.Errorf("fns Kubernetes: get %s kube pods failed, namespace was not found in kubernetes", ns)
		return
	}
	timeout := int64(3)
	list, listErr := pi.List(context.TODO(), metav1.ListOptions{
		TypeMeta:       metav1.TypeMeta{},
		LabelSelector:  labelSelector,
		TimeoutSeconds: &timeout,
		Limit:          0,
	})
	if listErr != nil {
		err = fmt.Errorf("fns Kubernetes: get %s kube pods failed, %v", ns, listErr)
		return
	}
	if list == nil || list.Items == nil || len(list.Items) == 0 {
		err = fmt.Errorf("fns Kubernetes: get %s kube pods failed, got empty pods", ns)
		return
	}
	groupPods = make(map[string]map[string]Pod)
	for _, item := range list.Items {
		fnsLabel, hasLabel := item.Labels[label]
		if !hasLabel {
			continue
		}
		namespaces := strings.Split(fnsLabel, ",")
		for _, namespace := range namespaces {
			namespace = strings.TrimSpace(namespace)
			group, hasGroup := groupPods[namespace]
			if !hasGroup {
				group = make(map[string]Pod)
			}
			podId := string(item.UID)
			_, hasPod := group[podId]
			if hasPod {
				continue
			}

			podIp := item.Status.PodIP
			podPort := 0
			for _, container := range item.Spec.Containers {
				for _, port := range container.Ports {
					if port.Name == label {
						podPort = int(port.ContainerPort)
						break
					}
				}
				if podPort > 0 {
					break
				}
			}
			if podPort == 0 {
				err = fmt.Errorf("fns Kubernetes: get %s kube pods failed, got fns pods but no fns port", ns)
				return
			}

			group[podId] = Pod{
				Id:      podIp,
				Name:    namespace,
				Address: fmt.Sprintf("%s:%d", podIp, podPort),
			}

			groupPods[namespace] = group

		}
	}
	return
}
