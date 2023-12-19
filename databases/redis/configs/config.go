package configs

import (
	"crypto/tls"
	"github.com/redis/rueidis"
	"net"
	"time"
)

type Option func(options *Options)

func WithSendToReplicas(fn func(cmd rueidis.Completed) bool) Option {
	return func(options *Options) {
		options.SendToReplicas = fn
	}
}

func WithAuthCredentials(fn func(rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error)) Option {
	return func(options *Options) {
		options.AuthCredentialsFn = fn
	}
}

func WithDialer(dialer *net.Dialer) Option {
	return func(options *Options) {
		options.Dialer = dialer
	}
}

func WithDialFn(fn func(string, *net.Dialer, *tls.Config) (conn net.Conn, err error)) Option {
	return func(options *Options) {
		options.DialFn = fn
	}
}

func WithSentinelDialer(dialer *net.Dialer) Option {
	return func(options *Options) {
		options.SentinelDialer = dialer
	}
}

func WithNewCacheStoreFn(fn rueidis.NewCacheStoreFn) Option {
	return func(options *Options) {
		options.NewCacheStoreFn = fn
	}
}

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

func (config *Config) AsOption(options Options) (option rueidis.ClientOption, err error) {
	option = rueidis.ClientOption{}
	if options.AuthCredentialsFn != nil {
		option.AuthCredentialsFn = options.AuthCredentialsFn
	}
	if options.Dialer != nil {
		option.Dialer = *options.Dialer
	}
	if options.DialFn != nil {
		option.DialFn = options.DialFn
	}
	if options.SendToReplicas != nil && !config.ReplicaOnly {
		option.SendToReplicas = options.SendToReplicas
	}
	if options.NewCacheStoreFn != nil {
		option.NewCacheStoreFn = options.NewCacheStoreFn
	}
	if config.Sentinel.Enable {
		option.Sentinel.MasterSet = config.Sentinel.MasterSet
		option.Sentinel.ClientName = config.Sentinel.ClientName
		option.Sentinel.Username = config.Sentinel.Username
		option.Sentinel.Password = config.Sentinel.Password
		if config.Sentinel.SSL.Enable {
			tlsConfig, tlsErr := config.Sentinel.SSL.Load()
			if tlsErr != nil {
				err = tlsErr
				return
			}
			option.Sentinel.TLSConfig = tlsConfig
		}
		if options.SentinelDialer != nil {
			option.Sentinel.Dialer = *options.SentinelDialer
		}
	}
	option.InitAddress = config.InitAddress
	option.Username = config.Username
	option.Password = config.Password
	option.ClientName = config.ClientName
	option.ClientSetInfo = config.ClientSetInfo
	option.ClientTrackingOptions = config.ClientTrackingOptions
	option.SelectDB = config.DB
	option.CacheSizeEachConn = config.CacheSizeEachConn
	option.RingScaleEachConn = config.RingScaleEachConn
	option.ReadBufferEachConn = config.ReadBufferEachConn
	option.WriteBufferEachConn = config.WriteBufferEachConn
	option.BlockingPoolSize = config.BlockingPoolSize
	option.PipelineMultiplex = config.PipelineMultiplex
	option.ConnWriteTimeout = config.ConnWriteTimeout
	option.MaxFlushDelay = config.MaxFlushDelay
	option.ShuffleInit = config.ShuffleInit
	option.ClientNoTouch = config.ClientNoTouch
	option.DisableRetry = config.DisableRetry
	option.DisableCache = config.DisableRetry
	option.AlwaysRESP2 = config.AlwaysRESP2
	option.AlwaysPipelining = config.AlwaysPipelining
	option.ForceSingleClient = config.ForceSingleClient
	option.ReplicaOnly = config.ReplicaOnly
	option.ClientNoEvict = config.ClientNoEvict
	if config.SSL.Enable {
		tlsConfig, tlsErr := config.SSL.Load()
		if tlsErr != nil {
			err = tlsErr
			return
		}
		option.TLSConfig = tlsConfig
	}
	return
}

func (config *Config) Make(options Options) (client rueidis.Client, err error) {
	opt, optErr := config.AsOption(options)
	if optErr != nil {
		err = optErr
		return
	}
	client, err = rueidis.NewClient(opt)
	return
}
