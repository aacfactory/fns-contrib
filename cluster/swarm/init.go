package swarm

import "github.com/aacfactory/fns/service"

func init() {
	service.RegisterClusterBuilder("swarm", ClusterBuilder)
}
