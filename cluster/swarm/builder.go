package swarm

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/shareds"
	"github.com/aacfactory/fns/service"
	"github.com/docker/docker/client"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func ClusterBuilder(options service.ClusterBuilderOptions) (cluster service.Cluster, err error) {
	config := Config{}
	configErr := options.Config.As(&config)
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
	cli.ServiceInspectWithRaw()
	cli.ServiceList()
	// todo ip use service.name ? id
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
	shared := shareds.Shared(shareds.Options{
		DefaultCacheTTL: 30 * time.Minute,
	})
	// cluster
	cluster = &Swarm{
		log:    options.Log.With("cluster", "swarm"),
		id:     options.AppId,
		host:   nodeIp,
		port:   options.Port,
		cli:    cli,
		shared: shared,
	}
	return
}
