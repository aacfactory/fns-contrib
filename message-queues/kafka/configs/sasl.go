package configs

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/sasl/plain"
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
	raw plain.Auth
}

func (plain *PlainSASL) Name() string {
	return "plain"
}

func (plain *PlainSASL) Construct(options MechanismOptions) (err error) {
	config := PlainSASLConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("kafka: construct plain sasl failed").WithCause(configErr)
		return
	}
	plain.raw.Zid = config.Zid
	plain.raw.User = config.Username
	plain.raw.Pass = config.Password
	return
}

func (plain *PlainSASL) Authenticate(ctx context.Context, host string) (Session, []byte, error) {
	return plain.raw.AsMechanism().Authenticate(ctx, host)
}
