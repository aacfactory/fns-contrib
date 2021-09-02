package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	rds "github.com/go-redis/redis/v8"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type Config struct {
	MasterSlaverMode   bool     `json:"masterSlaverMode,omitempty"`
	Network            string   `json:"network,omitempty"`
	Addr               []string `json:"addr,omitempty"`
	Username           string   `json:"username,omitempty"`
	Password           string   `json:"password,omitempty"`
	DB                 int      `json:"db,omitempty"`
	PoolSize           int      `json:"poolSize,omitempty"`
	SSL                bool     `json:"ssl,omitempty"`
	CaFilePath         string   `json:"caFilePath,omitempty"`
	CertFilePath       string   `json:"certFilePath,omitempty"`
	KeyFilePath        string   `json:"keyFilePath,omitempty"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify,omitempty"`
}

func (config *Config) CreateClient() (client Client, err error) {
	if config.Addr == nil || len(config.Addr) < 1 {
		err = fmt.Errorf("fns Redis Service: addr is empty")
		return
	}
	network := strings.TrimSpace(config.Network)
	if network == "" {
		network = "tcp"
	}
	username := strings.TrimSpace(config.Username)
	password := strings.TrimSpace(config.Password)
	db := config.DB
	poolSize := config.PoolSize
	if poolSize < 0 {
		poolSize = runtime.NumCPU()
	}
	var ssl *tls.Config
	if config.SSL {
		ssl, err = config.LoadSSL()
		if err != nil {
			return
		}
	}
	if config.MasterSlaverMode {
		if len(config.Addr) < 2 {
			err = fmt.Errorf("fns Redis Service: masterSlaverMode is enabled but num of addr is not gt 1")
			return
		}
		masterAddr := strings.TrimSpace(config.Addr[0])
		if masterAddr == "" {
			err = fmt.Errorf("fns Redis Service: masterSlaverMode is enabled but first of addr is empty")
			return
		}
		master := rds.NewClient(&rds.Options{
			Network:      network,
			Addr:         masterAddr,
			Username:     username,
			Password:     password,
			DB:           db,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			PoolSize:     poolSize,
			MinIdleConns: 1,
			TLSConfig:    ssl,
		})
		pingErr := master.Ping(context.TODO()).Err()
		if pingErr != nil {
			err = fmt.Errorf("fns Redis Service: ping %s failed, %v", masterAddr, pingErr)
			return
		}

		slaverAddrs := config.Addr[1:]
		slavers := make([]*rds.Client, 0, len(slaverAddrs))
		for _, slaverAddr := range slaverAddrs {
			slaverAddr = strings.TrimSpace(slaverAddr)
			if slaverAddr == "" {
				err = fmt.Errorf("fns Redis Service: masterSlaverMode is enabled but one of slavers addr is empty")
				return
			}
			slaver := rds.NewClient(&rds.Options{
				Network:      network,
				Addr:         slaverAddr,
				Username:     username,
				Password:     password,
				DB:           db,
				ReadTimeout:  2 * time.Second,
				WriteTimeout: 2 * time.Second,
				PoolSize:     poolSize,
				MinIdleConns: 1,
				TLSConfig:    ssl,
			})
			pingSlaverErr := slaver.Ping(context.TODO()).Err()
			if pingSlaverErr != nil {
				err = fmt.Errorf("fns Redis Service: ping %s failed, %v", slaverAddr, pingSlaverErr)
				return
			}
			slavers = append(slavers, slaver)
		}

		client = &MasterSlaver{
			master:     master,
			slavers:    slavers,
			slaversNum: len(slavers),
		}
		return
	}

	if len(config.Addr) == 1 {
		addr := strings.TrimSpace(config.Addr[0])
		if addr == "" {
			err = fmt.Errorf("fns Redis Service: first of addr is empty")
			return
		}
		node := rds.NewClient(&rds.Options{
			Network:      network,
			Addr:         addr,
			Username:     username,
			Password:     password,
			DB:           db,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			PoolSize:     poolSize,
			MinIdleConns: 1,
			TLSConfig:    ssl,
		})
		pingErr := node.Ping(context.TODO()).Err()
		if pingErr != nil {
			err = fmt.Errorf("fns Redis Service: ping %s failed, %v", addr, pingErr)
			return
		}
		client = &Standalone{
			client: node,
		}
		return
	}

	nodes := make([]*rds.Client, 0, len(config.Addr))
	for _, addr := range config.Addr {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			err = fmt.Errorf("fns Redis Service: one of addr is empty")
			return
		}
		node := rds.NewClient(&rds.Options{
			Network:      network,
			Addr:         addr,
			Username:     username,
			Password:     password,
			DB:           db,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			PoolSize:     poolSize,
			MinIdleConns: 1,
			TLSConfig:    ssl,
		})
		pingErr := node.Ping(context.TODO()).Err()
		if pingErr != nil {
			err = fmt.Errorf("fns Redis Service: ping %s failed, %v", addr, pingErr)
			return
		}
		nodes = append(nodes, node)
	}

	cluster := rds.NewClusterClient(&rds.ClusterOptions{
		Addrs:        config.Addr,
		Username:     username,
		Password:     password,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     poolSize,
		MinIdleConns: 1,
		TLSConfig:    ssl,
	})

	pingErr := cluster.Ping(context.TODO()).Err()
	if pingErr != nil {
		err = fmt.Errorf("fns Redis Service: ping %s failed, %v", config.Addr, pingErr)
		return
	}

	client = &Cluster{
		client: cluster,
	}

	return
}

func (config *Config) LoadSSL() (ssl *tls.Config, err error) {
	certFilePath := strings.TrimSpace(config.CertFilePath)
	if certFilePath == "" {
		err = fmt.Errorf("fns Redis Service: ssl is enabled but certFilePath is empty")
		return
	}
	keyFilePath := strings.TrimSpace(config.KeyFilePath)
	if keyFilePath == "" {
		err = fmt.Errorf("fns Redis Service: ssl is enabled but keyFilePath is empty")
		return
	}
	var absErr error
	certFilePath, absErr = filepath.Abs(certFilePath)
	if absErr != nil {
		err = fmt.Errorf("fns Redis Service: ssl is enabled but get absolute representation of certFilePath failed, %v", absErr)
		return
	}
	keyFilePath, absErr = filepath.Abs(keyFilePath)
	if absErr != nil {
		err = fmt.Errorf("fns Redis Service: ssl is enabled but get absolute representation of keyFilePath failed, %v", absErr)
		return
	}
	certificate, certificateErr := tls.LoadX509KeyPair(certFilePath, keyFilePath)
	if certificateErr != nil {
		err = fmt.Errorf("fns Redis Service: ssl is enabled but load x509 key pair failed, %v", certificateErr)
		return
	}

	ssl = &tls.Config{
		Certificates:       []tls.Certificate{certificate},
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	caFilePath := strings.TrimSpace(config.CaFilePath)
	if caFilePath != "" {
		caFilePath, absErr = filepath.Abs(caFilePath)
		if absErr != nil {
			err = fmt.Errorf("fns Redis Service: ssl is enabled but get absolute representation of caFilePath failed, %v", absErr)
			return
		}
		caContent, caReadErr := ioutil.ReadFile(caFilePath)
		if caReadErr != nil {
			err = fmt.Errorf("fns Redis Service: ssl is enabled but read caFilePath content failed, %v", caReadErr)
			return
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caContent) {
			err = fmt.Errorf("fns Redis Service: ssl is enabled but append ca into cert pool failed")
			return
		}
		ssl.RootCAs = caPool
	}
	return
}
