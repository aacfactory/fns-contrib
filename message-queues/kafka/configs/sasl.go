package configs

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"strings"
)

type SASLConfig struct {
	Name    string          `json:"name"`
	Options json.RawMessage `json:"options"`
}

func (config *SASLConfig) Config(log logs.Logger) (v Mechanism, err error) {
	name := strings.TrimSpace(config.Name)
	if name == "" {
		return
	}
	if len(config.Options) == 0 {
		config.Options = []byte("{}")
	}
	oc, ocErr := configures.NewJsonConfig(config.Options)
	if ocErr != nil {
		err = errors.Warning("kafka: invalid sasl config").WithMeta("sasl", name).WithCause(ocErr)
		return
	}
	switch name {
	case "plain":
		v = &PlainSASL{}
		break
	case "scram":
		v = &ScramSASL{}
		break
	default:
		r, has := mechanisms[name]
		if !has {
			err = errors.Warning("kafka: invalid sasl config").WithMeta("sasl", name).WithCause(fmt.Errorf("sasl was not found"))
			return
		}
		v = r
		break
	}
	err = v.Construct(MechanismOptions{
		Log:    log.With("sasl", name),
		Config: oc,
	})
	if err != nil {
		err = errors.Warning("kafka: invalid sasl config").WithMeta("sasl", name).WithCause(err)
		return
	}
	return
}

type Session interface {
	Challenge([]byte) (bool, []byte, error)
}

type MechanismOptions struct {
	Log    logs.Logger
	Config configures.Config
}

type Mechanism interface {
	Name() string
	Construct(options MechanismOptions) (err error)
	Authenticate(ctx context.Context, host string) (Session, []byte, error)
	Shutdown()
}

type kafkaMechanism struct {
	raw Mechanism
}

func (m *kafkaMechanism) Name() string {
	return m.raw.Name()
}

func (m *kafkaMechanism) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	s, p, err := m.raw.Authenticate(ctx, host)
	if err != nil {
		return nil, nil, err
	}
	var sess sasl.Session
	if s != nil {
		sess = sasl.Session(s)
	}
	return sess, p, nil
}

func (m *kafkaMechanism) Close() {
	m.raw.Shutdown()
}

func convert(mechanism Mechanism) (v sasl.Mechanism) {
	v = &kafkaMechanism{
		raw: mechanism,
	}
	return
}

var (
	mechanisms = make(map[string]Mechanism)
)

func RegisterSASL(mechanism Mechanism) {
	if mechanism == nil {
		return
	}
	mechanisms[mechanism.Name()] = mechanism
}

type PlainSASLConfig struct {
	Zid      string `json:"zid"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type PlainSASL struct {
	raw sasl.Mechanism
}

func (s *PlainSASL) Name() string {
	return "plain"
}

func (s *PlainSASL) Construct(options MechanismOptions) (err error) {
	config := PlainSASLConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("kafka: construct plain sasl failed").WithCause(configErr)
		return
	}
	auth := plain.Auth{}
	auth.Zid = config.Zid
	auth.User = config.Username
	auth.Pass = config.Password
	s.raw = auth.AsMechanism()
	return
}

func (s *PlainSASL) Authenticate(ctx context.Context, host string) (Session, []byte, error) {
	return s.raw.Authenticate(ctx, host)
}

func (s *PlainSASL) Shutdown() {
}

type ScramSASLConfig struct {
	Zid      string `json:"zid"`
	Username string `json:"username"`
	Password string `json:"password"`
	Algo     string `json:"algo"`
}

type ScramSASL struct {
	raw sasl.Mechanism
}

func (s *ScramSASL) Name() string {
	return "plain"
}

func (s *ScramSASL) Construct(options MechanismOptions) (err error) {
	config := ScramSASLConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("kafka: construct scram sasl failed").WithCause(configErr)
		return
	}
	auth := scram.Auth{}
	auth.Zid = config.Zid
	auth.User = config.Username
	auth.Pass = config.Password
	switch strings.TrimSpace(strings.ToUpper(config.Algo)) {
	case "SHA512":
		s.raw = auth.AsSha256Mechanism()
		break
	case "SHA256":
		s.raw = auth.AsSha256Mechanism()
		break
	default:
		err = errors.Warning("kafka: construct scram sasl failed").WithCause(fmt.Errorf("invalid algo"))
		return
	}
	return
}

func (s *ScramSASL) Authenticate(ctx context.Context, host string) (Session, []byte, error) {
	return s.raw.Authenticate(ctx, host)
}

func (s *ScramSASL) Shutdown() {
}
