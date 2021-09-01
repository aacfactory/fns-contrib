package kubernetes

import (
	"fmt"
	"github.com/aacfactory/fns"
	"strings"
)

const (
	kind  = "kubernetes"
	label = "fns"
)

func init() {
	fns.RegisterServiceDiscoveryRetriever(kind, Retriever)
}

func Retriever(option fns.ServiceDiscoveryOption) (discovery fns.ServiceDiscovery, err error) {

	config := Config{}
	configErr := option.Config.As(&config)
	if configErr != nil {
		err = fmt.Errorf("fns ServiceDiscovery KubernetesRetriever: read config failed, %v", configErr)
		return
	}

	namespace := strings.TrimSpace(config.Namespace)
	if namespace == "" {
		err = fmt.Errorf("fns ServiceDiscovery KubernetesRetriever: namespace in config is empty")
		return
	}

	discovery, err = newKube(namespace, option.HttpClientPoolSize)
	if err != nil {
		err = fmt.Errorf("fns ServiceDiscovery KubernetesRetriever: %v", err)
	}

	return
}
