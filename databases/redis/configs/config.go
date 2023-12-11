package configs

import (
	"crypto/tls"
	"github.com/redis/rueidis"
	"net"
	"time"
)

type Options struct {
	AuthCredentialsFn func(rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error)
	Dialer            *net.Dialer
	DialFn            func(string, *net.Dialer, *tls.Config) (conn net.Conn, err error)
	SentinelDialer    *net.Dialer
	SendToReplicas    func(cmd rueidis.Completed) bool
	NewCacheStoreFn   rueidis.NewCacheStoreFn
}

type SentinelConfig struct {
	Enable     bool      `json:"enable" yaml:"enable"`
	MasterSet  string    `json:"masterSet" yaml:"masterSet"`
	Username   string    `json:"username" yaml:"username"`
	Password   string    `json:"password" yaml:"password"`
	ClientName string    `json:"clientName" yaml:"clientName"`
	SSL        SSLConfig `json:"ssl" yaml:"ssl"`
}

type Config struct {
	InitAddress           []string       `json:"initAddress" yaml:"initAddress"`
	Addr                  []string       `json:"addr" yaml:"addr"`
	Username              string         `json:"username" yaml:"username"`
	Password              string         `json:"password" yaml:"password"`
	ClientName            string         `json:"clientName" yaml:"clientName"`
	ClientSetInfo         []string       `json:"clientSetInfo" yaml:"clientSetInfo"`
	ClientTrackingOptions []string       `json:"clientTrackingOptions" yaml:"clientTrackingOptions"`
	DB                    int            `json:"db" yaml:"db"`
	CacheSizeEachConn     int            `json:"cacheSizeEachConn" yaml:"cacheSizeEachConn"`
	RingScaleEachConn     int            `json:"ringScaleEachConn" yaml:"ringScaleEachConn"`
	ReadBufferEachConn    int            `json:"readBufferEachConn" yaml:"readBufferEachConn"`
	WriteBufferEachConn   int            `json:"writeBufferEachConn" yaml:"writeBufferEachConn"`
	BlockingPoolSize      int            `json:"blockingPoolSize" yaml:"blockingPoolSize"`
	PipelineMultiplex     int            `json:"pipelineMultiplex" yaml:"pipelineMultiplex"`
	ConnWriteTimeout      time.Duration  `json:"connWriteTimeout" yaml:"connWriteTimeout"`
	MaxFlushDelay         time.Duration  `json:"maxFlushDelay" yaml:"maxFlushDelay"`
	ShuffleInit           bool           `json:"shuffleInit" yaml:"shuffleInit"`
	ClientNoTouch         bool           `json:"clientNoTouch" yaml:"clientNoTouch"`
	DisableRetry          bool           `json:"disableRetry" yaml:"disableRetry"`
	DisableCache          bool           `json:"disableCache" yaml:"disableCache"`
	AlwaysPipelining      bool           `json:"alwaysPipelining" yaml:"alwaysPipelining"`
	AlwaysRESP2           bool           `json:"alwaysRESP2" yaml:"alwaysRESP2"`
	ForceSingleClient     bool           `json:"forceSingleClient" yaml:"forceSingleClient"`
	ReplicaOnly           bool           `json:"replicaOnly" yaml:"replicaOnly"`
	ClientNoEvict         bool           `json:"clientNoEvict" yaml:"clientNoEvict"`
	Sentinel              SentinelConfig `json:"sentinel" yaml:"sentinel"`
	SSL                   SSLConfig      `json:"ssl" yaml:"ssl"`
}

func (config *Config) Make(options Options) (client rueidis.Client, err error) {
	opt := rueidis.ClientOption{}
	if options.AuthCredentialsFn != nil {
		opt.AuthCredentialsFn = options.AuthCredentialsFn
	}
	if options.Dialer != nil {
		opt.Dialer = *options.Dialer
	}
	if options.DialFn != nil {
		opt.DialFn = options.DialFn
	}
	if options.SendToReplicas != nil && !config.ReplicaOnly {
		opt.SendToReplicas = options.SendToReplicas
	}
	if options.NewCacheStoreFn != nil {
		opt.NewCacheStoreFn = options.NewCacheStoreFn
	}
	if config.Sentinel.Enable {
		opt.Sentinel.MasterSet = config.Sentinel.MasterSet
		opt.Sentinel.ClientName = config.Sentinel.ClientName
		opt.Sentinel.Username = config.Sentinel.Username
		opt.Sentinel.Password = config.Sentinel.Password
		if config.Sentinel.SSL.Enable {
			tlsConfig, tlsErr := config.Sentinel.SSL.Config()
			if tlsErr != nil {
				err = tlsErr
				return
			}
			opt.Sentinel.TLSConfig = tlsConfig
		}
		if options.SentinelDialer != nil {
			opt.Sentinel.Dialer = *options.SentinelDialer
		}
	}
	opt.InitAddress = config.InitAddress
	opt.Username = config.Username
	opt.Password = config.Password
	opt.ClientName = config.ClientName
	opt.ClientSetInfo = config.ClientSetInfo
	opt.ClientTrackingOptions = config.ClientTrackingOptions
	opt.SelectDB = config.DB
	opt.CacheSizeEachConn = config.CacheSizeEachConn
	opt.RingScaleEachConn = config.RingScaleEachConn
	opt.ReadBufferEachConn = config.ReadBufferEachConn
	opt.WriteBufferEachConn = config.WriteBufferEachConn
	opt.BlockingPoolSize = config.BlockingPoolSize
	opt.PipelineMultiplex = config.PipelineMultiplex
	opt.ConnWriteTimeout = config.ConnWriteTimeout
	opt.MaxFlushDelay = config.MaxFlushDelay
	opt.ShuffleInit = config.ShuffleInit
	opt.ClientNoTouch = config.ClientNoTouch
	opt.DisableRetry = config.DisableRetry
	opt.DisableCache = config.DisableRetry
	opt.AlwaysRESP2 = config.AlwaysRESP2
	opt.AlwaysPipelining = config.AlwaysPipelining
	opt.ForceSingleClient = config.ForceSingleClient
	opt.ReplicaOnly = config.ReplicaOnly
	opt.ClientNoEvict = config.ClientNoEvict
	if config.SSL.Enable {
		tlsConfig, tlsErr := config.SSL.Config()
		if tlsErr != nil {
			err = tlsErr
			return
		}
		opt.TLSConfig = tlsConfig
	}
	client, err = rueidis.NewClient(opt)
	return
}
