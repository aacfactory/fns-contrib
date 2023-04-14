package swarm

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
)

type Swarm struct {
	log    logs.Logger
	id     string
	host   string
	port   int
	labels []string
	cli    *client.Client
	shared service.Shared
}

func (cluster *Swarm) Join(ctx context.Context) (err error) {
	// todo use shared cache to add this
	//TODO implement me
	panic("implement me")
}

func (cluster *Swarm) Leave(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Swarm) Nodes(ctx context.Context) (nodes service.Nodes, err error) {
	//TODO implement me
	// todo get from shared cache
	panic("implement me")
}

func (cluster *Swarm) Shared() (shared service.Shared) {
	shared = cluster.shared
	return
}

func (cluster *Swarm) findMembers(ctx context.Context) (addresses []string) {
	args := filters.NewArgs()
	for _, label := range cluster.labels {
		args.Add("label", label)
	}
	containers, listErr := cluster.cli.ContainerList(ctx, types.ContainerListOptions{
		Filters: args,
	})
	if listErr != nil {
		if cluster.log.ErrorEnabled() {
			cluster.log.Error().Caller().Cause(listErr).Message(fmt.Sprintf("%+v", errors.Warning("swarm: docker swarm find members failed").WithCause(listErr)))
		}
		return
	}
	addresses = make([]string, 0, 1)
	for _, container := range containers {
		if container.Ports == nil || len(container.Ports) == 0 {
			continue
		}
		cid := container.ID
		ip := container.Ports[0].IP
		if ip == "" {
			ip = cid
		}
		port := container.Ports[0].PrivatePort
		addresses = append(addresses, fmt.Sprintf("%s:%d", ip, port))
	}
	return
}
