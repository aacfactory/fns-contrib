package clusters

import (
	"fmt"
	"github.com/aacfactory/errors"
	rb "github.com/aacfactory/fns-contrib/databases/redis/barriers"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	rs "github.com/aacfactory/fns-contrib/databases/redis/shareds"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/clusters"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/redis/rueidis"
	"time"
)

const (
	name   = "redis"
	prefix = "fns/cluster/nodes/"
)

func init() {
	clusters.RegisterCluster("redis", &Cluster{})
}

var (
	opt = configs.Options{}
)

func Setup(options ...configs.Option) {
	for _, option := range options {
		option(&opt)
	}
}

type Cluster struct {
	log      logs.Logger
	client   rueidis.Client
	shared   shareds.Shared
	barrier  barriers.Barrier
	node     clusters.Node
	members  clusters.Nodes
	nodeKey  string
	ttl      time.Duration
	interval time.Duration
	joined   bool
	closeCh  chan struct{}
	events   chan clusters.NodeEvent
}

func (cluster *Cluster) Construct(options clusters.ClusterOptions) (err error) {
	cluster.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("cluster: construct failed").WithMeta("cluster", "redis").WithCause(configErr)
		return
	}
	client, clientErr := config.Make(opt)
	if clientErr != nil {
		err = errors.Warning("cluster: construct failed").WithMeta("cluster", "redis").WithCause(clientErr)
		return
	}
	shared, sharedErr := rs.NewWithClient(client)
	if sharedErr != nil {
		err = errors.Warning("cluster: construct failed").WithMeta("cluster", "redis").WithCause(sharedErr)
		return
	}
	barrier, barrierErr := rb.NewWithClient(client, config.Barrier.TTL)
	if barrierErr != nil {
		err = errors.Warning("cluster: construct failed").WithMeta("cluster", "redis").WithCause(barrierErr)
		return
	}

	cluster.client = client
	cluster.shared = shared
	cluster.barrier = barrier

	cluster.node = clusters.Node{
		Id:       options.Id,
		Version:  options.Version,
		Address:  options.Address,
		Services: make([]clusters.Service, 0, 1),
	}
	cluster.nodeKey = prefix + cluster.node.Id
	cluster.members = make(clusters.Nodes, 0, 1)

	cluster.ttl = config.KeepAlive.GetTTL()
	cluster.interval = config.KeepAlive.GetInterval()

	cluster.events = make(chan clusters.NodeEvent, 64)
	return
}

func (cluster *Cluster) AddService(service clusters.Service) {
	cluster.node.Services = append(cluster.node.Services, service)
	return
}

func (cluster *Cluster) Join(ctx context.Context) (err error) {
	if len(cluster.node.Services) > 0 {
		nodeBytes, encodeErr := json.Marshal(cluster.node)
		if encodeErr != nil {
			err = errors.Warning("cluster: join failed").WithMeta("cluster", "redis").WithCause(encodeErr)
			return
		}
		completed := cluster.client.B().Set().Key(cluster.nodeKey).Value(string(nodeBytes)).Ex(cluster.ttl).Build()
		setErr := cluster.client.Do(ctx, completed).Error()
		if setErr != nil {
			err = errors.Warning("cluster: join failed").WithMeta("cluster", "redis").WithCause(setErr)
			return
		}
		cluster.joined = true
	}
	cluster.closeCh = make(chan struct{}, 1)
	go cluster.listen()
	if cluster.log.DebugEnabled() {
		cluster.log.Debug().With("cluster", "join").Message("cluster: join succeed")
	}
	return
}

func (cluster *Cluster) Leave(ctx context.Context) (err error) {
	close(cluster.closeCh)
	if cluster.joined {
		rmErr := cluster.client.Do(ctx, cluster.client.B().Del().Key(cluster.nodeKey).Build()).Error()
		if rmErr != nil {
			err = errors.Warning("cluster: leave failed").WithMeta("cluster", "redis").WithCause(rmErr)
		}
	}
	cluster.client.Close()
	return
}

func (cluster *Cluster) NodeEvents() (events <-chan clusters.NodeEvent) {
	events = cluster.events
	return
}

func (cluster *Cluster) Shared() (shared shareds.Shared) {
	shared = cluster.shared
	return
}

func (cluster *Cluster) Barrier() (barrier barriers.Barrier) {
	barrier = cluster.barrier
	return
}

func (cluster *Cluster) listen() {
	ctx := context.TODO()
	stopped := false
	ttl := int64(cluster.ttl.Seconds())
	timer := time.NewTimer(cluster.interval)
	n := 0
	for {
		select {
		case <-cluster.closeCh:
			stopped = true
			break
		case <-timer.C:
			// expire
			if cluster.joined && n > 0 {
				expireErr := cluster.client.Do(ctx, cluster.client.B().Expire().Key(cluster.nodeKey).Seconds(ttl).Build()).Error()
				if cluster.log.DebugEnabled() {
					cluster.log.Debug().With("cluster", "keepalive").Message("cluster: keep succeed")
				}
				if expireErr != nil {
					if cluster.log.DebugEnabled() {
						cluster.log.Debug().With("cluster", "keepalive").Message("cluster: keep failed")
					}
					if cluster.log.ErrorEnabled() {
						cluster.log.Error().Cause(expireErr).With("redis", "expire").Message("cluster keepalive node failed")
					}
				} else {
					if cluster.log.DebugEnabled() {
						cluster.log.Debug().With("cluster", "keepalive").Message("cluster: keep succeed")
					}
				}
			}
			// list
			keys, keysErr := cluster.client.Do(ctx, cluster.client.B().Keys().Pattern(fmt.Sprintf("%s*", prefix)).Build()).AsStrSlice()
			if keysErr != nil && cluster.log.ErrorEnabled() {
				cluster.log.Error().Cause(keysErr).With("redis", "keys").Message("cluster get nodes failed")
				break
			}
			values, valuesErr := cluster.client.Do(ctx, cluster.client.B().Mget().Key(keys...).Build()).AsStrSlice()
			if valuesErr != nil {
				cluster.log.Error().Cause(valuesErr).With("redis", "mget").Message("cluster get nodes failed")
				break
			}
			news := make(clusters.Nodes, 0, len(values))
			for _, value := range values {
				node := clusters.Node{}
				decodeErr := json.Unmarshal(bytex.FromString(value), &node)
				if decodeErr != nil {
					if cluster.log.ErrorEnabled() {
						cluster.log.Error().Cause(decodeErr).With("node", "decode").Message("cluster get nodes failed")
					}
					continue
				}
				if node.Id == cluster.node.Id {
					continue
				}
				news = append(news, node)
			}
			events := news.Difference(cluster.members)
			for _, event := range events {
				cluster.events <- event
			}
			break
		}
		if stopped {
			break
		}
		timer.Reset(cluster.interval)
		if n == 0 {
			n++
		}
	}
	close(cluster.events)
	timer.Stop()
}
