package clusters

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/clusters"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/redis/rueidis"
)

type Cluster struct {
	client rueidis.Client
}

func (cluster *Cluster) Construct(options clusters.ClusterOptions) (err error) {
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("cluster: construct failed").WithMeta("cluster", "redis").WithCause(configErr)
		return
	}

	return
}

func (cluster *Cluster) AddService(service clusters.Service) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Cluster) Join(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Cluster) Leave(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Cluster) NodeEvents() (events <-chan clusters.NodeEvent) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Cluster) Shared() (shared shareds.Shared) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Cluster) Barrier() (barrier barriers.Barrier) {
	//TODO implement me
	panic("implement me")
}
