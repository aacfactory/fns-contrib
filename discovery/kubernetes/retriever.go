package kubernetes

import (
	"fmt"
	"github.com/aacfactory/fns"
	"strings"
	"time"
)

const (
	kind = "kubernetes"
	label = "fns"
)

func init() {
	fns.RegisterServiceDiscoveryRetriever(kind, Retriever)
}

func Retriever(option fns.ServiceDiscoveryOption) (discovery fns.ServiceDiscovery, err error) {

	config := Config{}
	configErr := option.Config.As(&config)
	if configErr != nil {
		err = fmt.Errorf("fns Kubernetes Discovery Retriever: read config failed, %v", configErr)
		return
	}

	namespace := strings.TrimSpace(config.Namespace)
	if namespace == "" {
		err = fmt.Errorf("fns Kubernetes Discovery Retriever: namespace in config is empty")
		return
	}

	checkingTTL := 1 * time.Minute

	checkingTimer := strings.TrimSpace(config.CheckingTimer)

	if checkingTimer != "" {
		checkingTTL0, parseErr := time.ParseDuration(checkingTimer)
		if parseErr != nil {
			err = fmt.Errorf("fns Kubernetes Discovery Retriever: checkingTimer in config is invalid, %v", parseErr)
			return
		}
		checkingTTL = checkingTTL0
	}

	discovery, err = newKube(namespace, checkingTTL)
	if err != nil {
		err = fmt.Errorf("fns Kubernetes Discovery Retriever: %v", err)
	}

	return
}
