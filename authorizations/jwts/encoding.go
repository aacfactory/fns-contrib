package jwts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
)

func New() services.Component {
	return &TokenEncoder{}
}

type TokenEncoder struct {
	raw *JWT
}

func (encoder *TokenEncoder) Name() (name string) {
	return "encoder"
}

func (encoder *TokenEncoder) Construct(options services.Options) (err error) {
	config := &Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("jwt: construct failed").WithCause(configErr)
		return
	}
	encoder.raw, err = config.CreateJWT()
	if err != nil {
		err = errors.Warning("jwt: construct failed").WithCause(err)
		return
	}
	return
}

func (encoder *TokenEncoder) Shutdown(_ context.Context) {

	return
}

func (encoder *TokenEncoder) Encode(_ context.Context, param authorizations.Authorization) (token authorizations.Token, err error) {
	signed, signErr := encoder.raw.Sign(param.Id.String(), param.Account.String(), param.Attributes, param.ExpireAT)
	if signErr != nil {
		err = errors.Warning("jwt: encode failed").WithCause(signErr)
		return
	}
	token = bytex.FromString(signed)
	return
}

func (encoder *TokenEncoder) Decode(_ context.Context, token authorizations.Token) (result authorizations.Authorization, err error) {
	id, account, attr, valid, claims, parseErr := encoder.raw.Parse(bytex.ToString(token))
	if parseErr != nil {
		err = errors.Warning("jwt: decode failed").WithCause(parseErr)
		return
	}
	if !valid {
		err = errors.Warning("jwt: decode failed").WithCause(fmt.Errorf("invalid"))
		return
	}
	result = authorizations.Authorization{
		Id:         authorizations.StringId(bytex.FromString(id)),
		Account:    authorizations.StringId(bytex.FromString(account)),
		Attributes: attr,
	}
	if claims.ExpiresAt != nil {
		result.ExpireAT = claims.ExpiresAt.Time
	}
	return
}
