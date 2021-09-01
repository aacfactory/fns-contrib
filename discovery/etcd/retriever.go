package etcd

import (
	"github.com/aacfactory/fns"
)

const (
	kind = "etcd"
)

func init() {
	fns.RegisterServiceDiscoveryRetriever(kind, Retriever)
}

func Retriever(option fns.ServiceDiscoveryOption) (discovery fns.ServiceDiscovery, err error) {
	discovery, err = newEtcdDiscovery(option)
	return
}
