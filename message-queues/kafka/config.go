package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/segmentio/kafka-go"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ClientTLSConfig struct {
	Enabled            bool   `json:"enabled"`
	CaFilePath         string `json:"caFilePath"`
	CertFilePath       string `json:"certFilePath"`
	KeyFilePath        string `json:"keyFilePath"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
}

func (ssl *ClientTLSConfig) Config() (config *tls.Config, err error) {
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

type OptionsConfig struct {
	SASLType       string          `json:"saslType"`
	Algo           string          `json:"algo"`
	Username       string          `json:"username"`
	Password       string          `json:"password"`
	ClientTLS      ClientTLSConfig `json:"clientTLS"`
	DualStack      bool            `json:"dualStack"`
	TimeoutSeconds int             `json:"timeoutSeconds"`
	ClientId       string          `json:"clientId"`
}

type Config struct {
	Brokers []string                `json:"brokers"`
	Options OptionsConfig           `json:"options"`
	Writer  map[string]WriterConfig `json:"writer"`
	Reader  map[string]ReaderConfig `json:"reader"`
}

type ReaderConfig struct {
	AutoCommit             bool          `json:"autoCommit"`
	GroupId                string        `json:"groupId"`
	Topics                 []string      `json:"topics"`
	Partition              int           `json:"partition"`
	QueueCapacity          int           `json:"queueCapacity"`
	MinBytes               int           `json:"minBytes"`
	MaxBytes               int           `json:"maxBytes"`
	MaxWait                time.Duration `json:"maxWait"`
	ReadLagInterval        time.Duration `json:"readLagInterval"`
	GroupBalancers         []string      `json:"groupBalancers"`
	HeartbeatInterval      time.Duration `json:"heartbeatInterval"`
	CommitInterval         time.Duration `json:"commitInterval"`
	PartitionWatchInterval time.Duration `json:"partitionWatchInterval"`
	WatchPartitionChanges  bool          `json:"watchPartitionChanges"`
	SessionTimeout         time.Duration `json:"sessionTimeout"`
	RebalanceTimeout       time.Duration `json:"rebalanceTimeout"`
	JoinGroupBackoff       time.Duration `json:"joinGroupBackoff"`
	RetentionTime          time.Duration `json:"retentionTime"`
	StartOffset            int64         `json:"startOffset"`
	ReadBackoffMin         time.Duration `json:"readBackoffMin"`
	ReadBackoffMax         time.Duration `json:"readBackoffMax"`
	IsolationLevel         string        `json:"isolationLevel"`
	MaxAttempts            int           `json:"maxAttempts"`
	OffsetOutOfRangeError  bool          `json:"offsetOutOfRangeError"`
}

func (config ReaderConfig) Isolation() (v kafka.IsolationLevel) {
	switch config.IsolationLevel {
	case "uncommitted":
		return kafka.ReadUncommitted
	default:
		return kafka.ReadCommitted
	}
}

func (config ReaderConfig) KafkaGroupBalancers() (v []kafka.GroupBalancer) {
	for _, balancer := range config.GroupBalancers {
		switch balancer {
		case "rack_affinity":
			v = append(v, &kafka.RackAffinityGroupBalancer{})
			break
		case "range":
			v = append(v, &kafka.RangeGroupBalancer{})
			break
		case "round_robin":
			v = append(v, &kafka.RoundRobinGroupBalancer{})
			break
		}
	}
	return
}

type ConsumerConfig struct {
	Handler        string          `json:"handler"`
	HandlerOptions json.RawMessage `json:"handlerOptions"`
	GroupId        string          `json:"groupId"`
	AutoCommit     bool            `json:"autoCommit"`
}

type WriterConfig struct {
	Compression string `json:"compression"`
	Balancer    string `json:"balancer"`
	RequiredAck string `json:"requiredAck"`
	MaxAttempts int    `json:"maxAttempts"`
	BatchSize   int    `json:"batchSize"`
	BatchBytes  int64  `json:"batchBytes"`
	Async       bool   `json:"async"`
}

func (config WriterConfig) CompressionKind() (compression kafka.Compression) {
	switch config.Compression {
	case "gzip":
		compression = kafka.Gzip
	case "snappy":
		compression = kafka.Snappy
	case "lz4":
		compression = kafka.Lz4
	case "zstd":
		compression = kafka.Zstd
	default:
		break
	}
	return
}

func (config WriterConfig) Balance() (balancer kafka.Balancer) {
	switch config.Balancer {
	case "round_robin":
		balancer = &kafka.RoundRobin{}
	case "hash":
		balancer = &kafka.Hash{}
	case "reference_hash":
		balancer = &kafka.ReferenceHash{}
	case "crc32":
		balancer = &kafka.CRC32Balancer{}
	case "murmur2":
		balancer = &kafka.Murmur2Balancer{}
	case "least":
		balancer = &kafka.LeastBytes{}
	default:
		balancer = &kafka.LeastBytes{}
		break
	}
	return
}

func (config WriterConfig) Ack() kafka.RequiredAcks {
	switch config.RequiredAck {
	case "all":

		return kafka.RequireAll
	case "one":

		return kafka.RequireOne
	default:
		return kafka.RequireNone
	}
}
