package kubernetes

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/cluster"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/logs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	cluster.RegisterBootstrap("kubernetes", &bootstrap{})
}

type bootstrap struct {
	log                logs.Logger
	id                 string
	ip                 string
	namespace          string
	labels             []string
	listTimeoutSeconds int64
	client             *kube.Clientset
}

func (b *bootstrap) Build(options cluster.BootstrapOptions) (err error) {
	b.log = options.Log.With("cluster", "kubernetes")
	config := Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(configErr)
		return
	}
	namespace := strings.TrimSpace(config.Namespace)
	if namespace == "" {
		err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(fmt.Errorf("namespace is required"))
		return
	}
	b.namespace = namespace
	if config.Labels == nil {
		config.Labels = []string{"FNS-SERVICE"}
	}
	timeout := config.TimeoutSeconds
	if timeout < 1 {
		timeout = 60
	}
	b.listTimeoutSeconds = int64(timeout)
	b.labels = make([]string, 0, 1)
	for _, label := range config.Labels {
		label = strings.TrimSpace(label)
		if label != "" {
			b.labels = append(b.labels, label)
		}
	}
	if len(b.labels) == 0 {
		err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(fmt.Errorf("labels are required"))
		return
	}
	var kubConfig *rest.Config
	if config.InCluster {
		kubConfig, err = rest.InClusterConfig()
		if err != nil {
			err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(err)
			return
		}
		id, _ := os.LookupEnv("MY_POD_NAME")
		id = strings.TrimSpace(id)
		if id == "" {
			err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(fmt.Errorf("can not find MY_POD_NAME env, please set MY_POD_NAME via inject"))
			return
		}
		b.id = id
		ip, _ := os.LookupEnv("MY_POD_IP")
		ip = strings.TrimSpace(ip)
		if ip == "" {
			err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(fmt.Errorf("can not find MY_POD_IP env, please set MY_POD_IP via inject"))
			return
		}
		b.ip = ip
	} else {
		configPath := strings.TrimSpace(config.KubeConfigPath)
		if configPath == "" {
			err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(fmt.Errorf("kubeConfigPath is required"))
			return
		}
		kubConfig, err = clientcmd.BuildConfigFromFlags("", filepath.Join(configPath, "config"))
		if err != nil {
			err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(err)
			return
		}
		b.id = uid.UID()
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = os.Getenv("HOSTNAME")
			if hostname == "" {
				err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(fmt.Errorf("can not get hostname, please set HOSTNAME env which can be ip"))
			}
			return
		}
		ips, ipErr := net.LookupIP(hostname)
		if ipErr != nil {
			err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(ipErr)
			return
		}
		for _, ip := range ips {
			if ip.IsGlobalUnicast() {
				b.ip = ip.To4().String()
				break
			}
		}
	}
	client, createErr := kube.NewForConfig(kubConfig)
	if createErr != nil {
		err = errors.Warning("fns: build kubernetes bootstrap failed").WithCause(createErr)
		return
	}
	b.client = client
	return
}

func (b *bootstrap) Id() (id string) {
	id = b.id
	return
}

func (b *bootstrap) Ip() (ip string) {
	ip = b.ip
	return
}

func (b *bootstrap) FindMembers(ctx context.Context) (addresses []string) {
	pods := b.client.CoreV1().Pods(b.namespace)
	results, listErr := pods.List(ctx, metav1.ListOptions{
		TypeMeta:       metav1.TypeMeta{},
		LabelSelector:  strings.Join(b.labels, ","),
		TimeoutSeconds: &b.listTimeoutSeconds,
		Limit:          0,
	})
	if listErr != nil {
		if b.log.ErrorEnabled() {
			b.log.Error().Caller().Cause(listErr).Message(fmt.Sprintf("%+v", errors.Warning("fns: kubernetes find members failed").WithCause(listErr)))
		}
		return
	}
	if results == nil {
		return
	}
	if results.Items == nil || len(results.Items) == 0 {
		return
	}
	addresses = make([]string, 0, 1)
	for _, pod := range results.Items {
		ip := pod.Status.PodIP
		port := 0
		for _, container := range pod.Spec.Containers {
			ports := container.Ports
			if ports == nil || len(ports) == 0 {
				continue
			}
			port = int(ports[0].ContainerPort)
			break
		}
		if port == 0 {
			continue
		}

		addresses = append(addresses, fmt.Sprintf("%s:%d", ip, port))
	}
	if b.log.DebugEnabled() {
		b.log.Debug().Caller().Message(fmt.Sprintf("fns: kubernetes find members, %v", addresses))
	}
	return
}
