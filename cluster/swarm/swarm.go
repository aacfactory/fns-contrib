package swarm

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/shareds"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func ClusterBuilder(options service.ClusterBuilderOptions) (cluster service.Cluster, err error) {
	log := options.Log.With("cluster", "swarm")
	config := Config{}
	configErr := json.Unmarshal(options.Config.Options, &config)
	if configErr != nil {
		err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(configErr)
		return
	}
	if config.Labels == nil {
		config.Labels = []string{"FNS-SERVICE"}
	}
	labels := make([]string, 0, 1)
	for _, label := range config.Labels {
		label = strings.TrimSpace(label)
		if label != "" {
			labels = append(labels, label)
		}
	}
	if len(labels) == 0 {
		err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(fmt.Errorf("labels are required"))
		return
	}
	var cli *client.Client
	if config.FromENV {
		cli, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(err)
			return
		}
	} else {
		cli, err = client.NewClientWithOpts(
			client.WithHost(config.Host),
			client.WithTLSClientConfig(filepath.Join(config.CertDir, "ca.pem"), filepath.Join(config.CertDir, "cert.pem"), filepath.Join(config.CertDir, "key.pem")),
		)
		if err != nil {
			err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(err)
			return
		}
	}
	id := options.AppId
	hostname, hasHostname := os.LookupEnv("HOSTNAME")
	if !hasHostname {
		hostname, err = os.Hostname()
		if err != nil {
			err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(err)
			return
		}
	}
	ips, ipErr := net.LookupIP(hostname)
	if ipErr != nil {
		err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(ipErr)
		return
	}
	nodeIp := ""
	for _, ip := range ips {
		if ip.IsGlobalUnicast() {
			nodeIp = ip.To4().String()
			break
		}
	}
	if nodeIp == "" {
		err = errors.Warning("swarm: build docker swarm bootstrap failed").WithCause(fmt.Errorf("can not get ip from %s", hostname))
		return
	}
	// shared
	shared := shareds.Shared()
	// todo proxy

	return
}

type Swarm struct {
	log          logs.Logger
	id           string
	ip           string
	labels       []string
	proxyAddress string
	proxyTLS     *service.TLSConfig
	cli          *client.Client
	shared       service.Shared
}

func (cluster *Swarm) Join(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Swarm) Leave(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (cluster *Swarm) Nodes(ctx context.Context) (nodes service.Nodes, err error) {
	//TODO implement me
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
