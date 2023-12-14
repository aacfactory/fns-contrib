package hazelcasts

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/cluster/hazelcasts/configs"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/clusters"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

const (
	name   = "hazelcast"
	prefix = "fns:cluster:nodes:"
)

func init() {
	clusters.RegisterCluster(name, &Cluster{})
}

type Cluster struct {
	log      logs.Logger
	client   *hazelcast.Client
	shared   shareds.Shared
	barrier  barriers.Barrier
	node     clusters.Node
	members  clusters.Nodes
	nodes    *hazelcast.Map
	nodeKey  string
	ttl      time.Duration
	interval time.Duration
	joined   bool
	closeCh  chan struct{}
	events   chan clusters.NodeEvent
}

func (cluster *Cluster) Construct(options clusters.ClusterOptions) (err error) {
	cluster.log = options.Log.With("cluster", name)
	config := configs.Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("hazelcast: construct failed").WithCause(configErr)
		return
	}
	conf, confErr := config.As()
	if confErr != nil {
		err = errors.Warning("hazelcast: construct failed").WithCause(confErr)
		return
	}
	conf.Logger.CustomLogger = &log{
		raw: cluster.log.With("hazelcast", "logger"),
	}

	client, clientErr := hazelcast.StartNewClientWithConfig(context.TODO(), conf)
	if clientErr != nil {
		err = errors.Warning("hazelcast: construct failed").WithCause(clientErr)
		return
	}
	cluster.client = client
	// shared
	sharedConfig, shardConfigErr := config.SharedConfig()
	if shardConfigErr != nil {
		err = errors.Warning("hazelcast: construct failed").WithCause(shardConfigErr)
		return
	}
	shared := extraShared
	if shared == nil {
		shared = NewShared(client)
	}
	sharedErr := shared.Construct(shareds.Options{
		Log:    cluster.log.With("shared", "hazelcast"),
		Config: sharedConfig,
	})
	if sharedErr != nil {
		err = errors.Warning("hazelcast: construct failed").WithCause(sharedErr)
		return
	}
	cluster.shared = shared
	// barrier
	if extraBarrier == nil {
		barrierConfig, barrierConfigErr := config.BarrierConfig()
		if barrierConfigErr != nil {
			err = errors.Warning("hazelcast: construct failed").WithCause(barrierConfigErr)
			return
		}
		bc := configs.BarrierConfig{}
		bcErr := barrierConfig.As(&bc)
		if bcErr != nil {
			err = errors.Warning("hazelcast: construct failed").WithCause(bcErr)
			return
		}
		barrier, barrierErr := NewBarrier(context.TODO(), client, bc.Size)
		if barrierErr != nil {
			err = errors.Warning("hazelcast: construct failed").WithCause(barrierErr)
			return
		}
		cluster.barrier = barrier
	} else {
		barrierConfig, barrierConfigErr := config.BarrierConfig()
		if barrierConfigErr != nil {
			err = errors.Warning("hazelcast: construct failed").WithCause(barrierConfigErr)
			return
		}
		barrier, barrierErr := extraBarrier.Build(context.TODO(), barrierConfig)
		if barrierErr != nil {
			err = errors.Warning("hazelcast: construct failed").WithCause(barrierErr)
			return
		}
		cluster.barrier = barrier
	}

	cluster.node = clusters.Node{
		Id:       options.Id,
		Version:  options.Version,
		Address:  options.Address,
		Services: make([]clusters.Service, 0, 1),
	}
	cluster.nodeKey = prefix + cluster.node.Id
	cluster.members = make(clusters.Nodes, 0, 1)

	cluster.nodes, err = client.GetMap(context.TODO(), "fns:cluster:nodes")
	if err != nil {
		err = errors.Warning("hazelcast: construct failed").WithCause(err)
		return
	}

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
			err = errors.Warning("cluster: join failed").WithMeta("cluster", name).WithCause(encodeErr)
			return
		}
		setErr := cluster.nodes.SetWithTTL(ctx, cluster.nodeKey, string(nodeBytes), cluster.ttl)
		if setErr != nil {
			err = errors.Warning("cluster: join failed").WithMeta("cluster", name).WithCause(setErr)
			return
		}
		cluster.joined = true
	}
	cluster.closeCh = make(chan struct{}, 1)

	go cluster.listen()

	if cluster.log.DebugEnabled() {
		cluster.log.Debug().With("action", "join").Message("cluster: join succeed")
	}
	return
}

func (cluster *Cluster) Leave(ctx context.Context) (err error) {
	close(cluster.closeCh)
	errs := errors.MakeErrors()
	if cluster.joined {
		_, rmErr := cluster.nodes.Remove(ctx, cluster.nodeKey)
		if rmErr != nil {
			errs.Append(rmErr)
		}
	}
	shutdownErr := cluster.client.Shutdown(ctx)
	if shutdownErr != nil {
		errs.Append(shutdownErr)
	}
	if len(errs) > 0 {
		err = errors.Warning("cluster: leave failed").WithMeta("cluster", name).WithCause(errs.Error())
		return
	}
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
	timer := time.NewTimer(cluster.interval)
	for {
		select {
		case <-cluster.closeCh:
			stopped = true
			break
		case <-timer.C:
			// expire
			if cluster.joined {
				expireErr := cluster.nodes.SetTTL(ctx, cluster.nodeKey, cluster.ttl)
				if expireErr != nil {
					if cluster.log.DebugEnabled() {
						cluster.log.Debug().With("action", "keepalive").Message("cluster: keepalive failed")
					}
					if cluster.log.ErrorEnabled() {
						cluster.log.Error().Cause(expireErr).With("hazelcast", "setTTL").Message("cluster: keepalive failed")
					}
				} else {
					if cluster.log.DebugEnabled() {
						view, viewErr := cluster.nodes.GetEntryView(ctx, cluster.nodeKey)
						if viewErr != nil {
							cluster.log.Debug().Cause(viewErr).With("cluster", "keepalive").Message("cluster: get node ttl failed")
						}
						if view != nil {
							cluster.log.Debug().With("action", "keepalive").With("ttl", time.Duration(view.TTL)*time.Millisecond).Message("cluster: keepalive succeed")
						}
					}
				}

			}
			// list
			values, valuesErr := cluster.nodes.GetValues(ctx)
			if valuesErr != nil {
				cluster.log.Error().Cause(valuesErr).With("action", "values").Message("cluster get nodes failed")
				break
			}

			news := make(clusters.Nodes, 0, 8)
			if len(values) > 0 {
				for _, value := range values {
					s, ok := value.(string)
					if !ok {
						continue
					}
					node := clusters.Node{}
					decodeErr := json.Unmarshal(bytex.FromString(s), &node)
					if decodeErr != nil {
						if cluster.log.ErrorEnabled() {
							cluster.log.Error().Cause(decodeErr).With("action", "decode").Message("cluster get nodes failed")
						}
						continue
					}
					if node.Id == cluster.node.Id {
						continue
					}
					news = news.Add(node)
				}
			}
			events := news.Difference(cluster.members)
			for _, event := range events {
				cluster.events <- event
			}
			cluster.members = news
			break
		}
		if stopped {
			break
		}
		timer.Reset(cluster.interval)
	}
	close(cluster.events)
	timer.Stop()
}
