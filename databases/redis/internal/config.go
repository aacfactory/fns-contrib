package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/aacfactory/errors"
	rds "github.com/redis/go-redis/v9"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type SSLConfig struct {
	CaFilePath         string `json:"caFilePath"`
	CertFilePath       string `json:"certFilePath"`
	KeyFilePath        string `json:"keyFilePath"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
}

func (ssl *SSLConfig) Config() (config *tls.Config, err error) {
	cas := x509.NewCertPool()
	if ssl.CaFilePath != "" {
		path := strings.TrimSpace(ssl.CaFilePath)
		if !filepath.IsAbs(path) {
			path, err = filepath.Abs(path)
			if err != nil {
				err = errors.Warning("get absolute representation of path failed").WithMeta("path", path).WithCause(err)
				return
			}
		}
		path = filepath.ToSlash(path)
		p, readErr := os.ReadFile(path)
		if readErr != nil {
			err = errors.Warning("read file failed").WithMeta("path", path).WithCause(readErr)
			return
		}
		cas.AppendCertsFromPEM(p)
	}
	cert := strings.TrimSpace(ssl.CertFilePath)
	if cert == "" {
		err = errors.Warning("cert file path is required")
		return
	}
	if !filepath.IsAbs(cert) {
		cert, err = filepath.Abs(cert)
		if err != nil {
			err = errors.Warning("get absolute representation of path failed").WithMeta("path", cert).WithCause(err)
			return
		}
	}
	cert = filepath.ToSlash(cert)
	certPEM, readCertErr := os.ReadFile(cert)
	if readCertErr != nil {
		err = errors.Warning("read file failed").WithMeta("path", cert).WithCause(readCertErr)
		return
	}
	key := strings.TrimSpace(ssl.KeyFilePath)
	if key == "" {
		err = errors.Warning("key file path is required")
		return
	}
	if !filepath.IsAbs(key) {
		key, err = filepath.Abs(key)
		if err != nil {
			err = errors.Warning("get absolute representation of path failed").WithMeta("path", key).WithCause(err)
			return
		}
	}
	key = filepath.ToSlash(key)
	keyPEM, readKeyErr := os.ReadFile(key)
	if readKeyErr != nil {
		err = errors.Warning("read file failed").WithMeta("path", key).WithCause(readKeyErr)
		return
	}
	certificate, certificateErr := tls.X509KeyPair(certPEM, keyPEM)
	if certificateErr != nil {
		err = errors.Warning("make x509 keypair failed").WithCause(certificateErr)
		return
	}
	config = &tls.Config{
		RootCAs:            cas,
		Certificates:       []tls.Certificate{certificate},
		InsecureSkipVerify: ssl.InsecureSkipVerify,
	}
	return
}

type Config struct {
	Addr            []string   `json:"addr"`
	Username        string     `json:"username"`
	Password        string     `json:"password"`
	DB              int        `json:"db"`
	PoolSize        int        `json:"poolSize"`
	PoolTimeout     string     `json:"poolTimeout"`
	MaxRetries      int        `json:"maxRetries"`
	MinRetryBackoff string     `json:"minRetryBackoff"`
	MaxRetryBackoff string     `json:"maxRetryBackoff"`
	DialTimeout     string     `json:"dialTimeout"`
	ReadTimeout     string     `json:"readTimeout"`
	WriteTimeout    string     `json:"writeTimeout"`
	MinIdleConns    int        `json:"minIdleConns"`
	MaxIdleConns    int        `json:"maxIdleConns"`
	ConnMaxIdleTime string     `json:"connMaxIdleTime"`
	ConnMaxLifetime string     `json:"connMaxLifetime"`
	SSL             *SSLConfig `json:"ssl"`
}

func (config *Config) options(appId string, appName string) (options interface{}, err error) {
	if config.Addr == nil || len(config.Addr) == 0 {
		err = errors.Warning("addr is required")
		return
	}
	addrs := make([]string, 0, 1)
	for _, addr := range config.Addr {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		addrs = append(addrs, addr)
	}
	if len(addrs) == 0 {
		err = errors.Warning("addr is required")
		return
	}
	username := strings.TrimSpace(config.Username)
	password := strings.TrimSpace(config.Password)
	db := config.DB
	if db < 0 {
		err = errors.Warning("db is invalid")
		return
	}
	poolSize := config.PoolSize
	if poolSize < 1 {
		poolSize = 64
	}
	poolTimeout := time.Duration(0)
	if config.PoolTimeout != "" {
		poolTimeout, err = time.ParseDuration(strings.TrimSpace(config.PoolTimeout))
		if err != nil {
			err = errors.Warning("poolTimeout must be time.Duration format").WithCause(err)
			return
		}
	}
	maxRetries := config.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	minRetryBackoff := time.Duration(0)
	if config.MinRetryBackoff != "" {
		minRetryBackoff, err = time.ParseDuration(strings.TrimSpace(config.MinRetryBackoff))
		if err != nil {
			err = errors.Warning("minRetryBackoff must be time.Duration format").WithCause(err)
			return
		}
	}
	maxRetryBackoff := time.Duration(0)
	if config.MaxRetryBackoff != "" {
		maxRetryBackoff, err = time.ParseDuration(strings.TrimSpace(config.MaxRetryBackoff))
		if err != nil {
			err = errors.Warning("maxRetryBackoff must be time.Duration format").WithCause(err)
			return
		}
	}
	dialTimeout := time.Duration(0)
	if config.DialTimeout != "" {
		dialTimeout, err = time.ParseDuration(strings.TrimSpace(config.DialTimeout))
		if err != nil {
			err = errors.Warning("dialTimeout must be time.Duration format").WithCause(err)
			return
		}
	}
	readTimeout := time.Duration(0)
	if config.ReadTimeout != "" {
		readTimeout, err = time.ParseDuration(strings.TrimSpace(config.ReadTimeout))
		if err != nil {
			err = errors.Warning("readTimeout must be time.Duration format").WithCause(err)
			return
		}
	}
	writeTimeout := time.Duration(0)
	if config.WriteTimeout != "" {
		writeTimeout, err = time.ParseDuration(strings.TrimSpace(config.WriteTimeout))
		if err != nil {
			err = errors.Warning("writeTimeout must be time.Duration format").WithCause(err)
			return
		}
	}
	minIdleConns := config.MinIdleConns
	if minIdleConns < 1 {
		minIdleConns = 2
	}
	maxIdleConns := config.MaxIdleConns
	if maxIdleConns < 1 {
		maxIdleConns = 8
	}
	connMaxIdleTime := time.Duration(0)
	if config.ConnMaxIdleTime != "" {
		connMaxIdleTime, err = time.ParseDuration(strings.TrimSpace(config.ConnMaxIdleTime))
		if err != nil {
			err = errors.Warning("connMaxIdleTime must be time.Duration format").WithCause(err)
			return
		}
	}
	connMaxLifetime := time.Duration(0)
	if config.ConnMaxLifetime != "" {
		connMaxLifetime, err = time.ParseDuration(strings.TrimSpace(config.ConnMaxLifetime))
		if err != nil {
			err = errors.Warning("connMaxLifetime must be time.Duration format").WithCause(err)
			return
		}
	}
	var tlsConfig *tls.Config
	if config.SSL != nil {
		tlsConfig, err = config.SSL.Config()
		if err != nil {
			err = errors.Warning("ssl is invalid").WithCause(err)
			return
		}
	}
	if len(addrs) == 1 {
		options = &rds.Options{
			Network:               "",
			Addr:                  addrs[0],
			ClientName:            fmt.Sprintf("fns:%s:%s", appName, appId),
			Dialer:                nil,
			OnConnect:             nil,
			Username:              username,
			Password:              password,
			CredentialsProvider:   nil,
			DB:                    db,
			MaxRetries:            maxRetries,
			MinRetryBackoff:       minRetryBackoff,
			MaxRetryBackoff:       maxRetryBackoff,
			DialTimeout:           dialTimeout,
			ReadTimeout:           readTimeout,
			WriteTimeout:          writeTimeout,
			ContextTimeoutEnabled: true,
			PoolFIFO:              true,
			PoolSize:              poolSize,
			PoolTimeout:           poolTimeout,
			MinIdleConns:          minIdleConns,
			MaxIdleConns:          maxIdleConns,
			ConnMaxIdleTime:       connMaxIdleTime,
			ConnMaxLifetime:       connMaxLifetime,
			TLSConfig:             tlsConfig,
			Limiter:               nil,
		}
	} else {
		options = &rds.ClusterOptions{
			Addrs:                 nil,
			ClientName:            fmt.Sprintf("fns:%s:%s", appName, appId),
			NewClient:             nil,
			MaxRedirects:          0,
			ReadOnly:              false,
			RouteByLatency:        false,
			RouteRandomly:         false,
			ClusterSlots:          nil,
			Dialer:                nil,
			OnConnect:             nil,
			Username:              username,
			Password:              password,
			MaxRetries:            maxRetries,
			MinRetryBackoff:       minRetryBackoff,
			MaxRetryBackoff:       maxRetryBackoff,
			DialTimeout:           dialTimeout,
			ReadTimeout:           readTimeout,
			WriteTimeout:          writeTimeout,
			ContextTimeoutEnabled: true,
			PoolFIFO:              true,
			PoolSize:              poolSize,
			PoolTimeout:           poolTimeout,
			MinIdleConns:          minIdleConns,
			MaxIdleConns:          maxIdleConns,
			ConnMaxIdleTime:       connMaxIdleTime,
			ConnMaxLifetime:       connMaxLifetime,
			TLSConfig:             tlsConfig,
		}
	}
	return
}

func (config *Config) Dial(appId string, appName string) (client Client, err error) {
	opts, optsErr := config.options(appId, appName)
	if optsErr != nil {
		err = errors.Warning("redis: dial failed").WithCause(optsErr)
		return
	}
	switch opts.(type) {
	case *rds.Options:
		client = rds.NewClient(opts.(*rds.Options))
		pingErr := client.Ping(context.TODO()).Err()
		if pingErr != nil {
			err = errors.Warning("redis: dial failed").WithCause(pingErr)
			return
		}
		break
	case *rds.ClusterOptions:
		client = rds.NewClusterClient(opts.(*rds.ClusterOptions))
		pingErr := client.Ping(context.TODO()).Err()
		if pingErr != nil {
			err = errors.Warning("redis: dial failed").WithCause(pingErr)
			return
		}
		break
	default:
		err = errors.Warning("redis: dial failed").WithCause(errors.Warning("unknown options type").WithMeta("type", reflect.TypeOf(opts).String()))
		return
	}
	return
}
