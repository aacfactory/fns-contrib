package swarm

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/cluster"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/logs"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	cluster.RegisterBootstrap("swarm", &bootstrap{})
}

type bootstrap struct {
	log    logs.Logger
	cli    *client.Client
	id     string
	ip     string
	labels []string
}

func (b *bootstrap) Build(options cluster.BootstrapOptions) (err error) {
	b.log = options.Log.With("cluster", "swarm")
	config := Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(configErr)
		return
	}
	if config.Labels == nil {
		config.Labels = []string{"FNS-SERVICE"}
	}
	b.labels = make([]string, 0, 1)
	for _, label := range config.Labels {
		label = strings.TrimSpace(label)
		if label != "" {
			b.labels = append(b.labels, label)
		}
	}
	if len(b.labels) == 0 {
		err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(fmt.Errorf("labels are required"))
		return
	}
	var cli *client.Client
	if config.FromENV {
		cli, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(err)
			return
		}
	} else {
		cli, err = client.NewClientWithOpts(
			client.WithHost(config.Host),
			client.WithTLSClientConfig(filepath.Join(config.CertDir, "ca.pem"), filepath.Join(config.CertDir, "cert.pem"), filepath.Join(config.CertDir, "key.pem")),
		)
		if err != nil {
			err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(err)
			return
		}
	}
	b.cli = cli
	hostname, hasHostname := os.LookupEnv("HOSTNAME")
	if !hasHostname {
		b.id = uid.UID()
		hostname, err = os.Hostname()
		if err != nil {
			err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(err)
			return
		}
	} else {
		b.id = hostname
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
	if b.ip == "" {
		err = errors.Warning("fns: build docker swarm bootstrap failed").WithCause(fmt.Errorf("can not get ip from %s", hostname))
		return
	}
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
	args := filters.NewArgs()
	for _, label := range b.labels {
		args.Add("label", label)
	}
	containers, listErr := b.cli.ContainerList(ctx, types.ContainerListOptions{
		Filters: args,
	})
	if listErr != nil {
		if b.log.ErrorEnabled() {
			b.log.Error().Caller().Cause(listErr).Message(fmt.Sprintf("%+v", errors.Warning("fns: docker swarm find members failed").WithCause(listErr)))
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
