package jwts

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/authorizations"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"strings"
)

func Tokens() authorizations.Tokens {
	return &jwtTokens{}
}

type jwtTokens struct {
	log  logs.Logger
	core *JWT
}

func (tokens *jwtTokens) Name() (name string) {
	name = "jwt"
	return
}

func (tokens *jwtTokens) Build(options service.ComponentOptions) (err error) {
	tokens.log = options.Log
	config := &Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("jwt: build failed").WithCause(configErr)
		return
	}
	tokens.core, err = config.CreateJWT()
	if err != nil {
		err = errors.Warning("jwt: build failed").WithCause(err)
		return
	}
	return
}

func (tokens *jwtTokens) Create(_ context.Context, param authorizations.CreateTokenParam) (token authorizations.Token, err errors.CodeError) {
	if param.Id == "" {
		err = errors.Warning("jwt: create token failed").WithCause(errors.Warning("id is required"))
		return
	}
	if !param.UserId.Exist() {
		err = errors.Warning("jwt: create token failed").WithCause(errors.Warning("user id is required"))
		return
	}
	attr := param.Attributes
	if attr == nil {
		attr = json.NewObject()
	}
	id := param.Id
	signed, signErr := tokens.core.Sign(id, param.UserId, attr)
	if signErr != nil {
		err = errors.Warning("jwt: create token failed").WithCause(signErr)
		return
	}
	token = authorizations.Token(fmt.Sprintf("Bearer %s", signed))
	return
}

func (tokens *jwtTokens) Parse(_ context.Context, token authorizations.Token) (result authorizations.ParsedToken, err errors.CodeError) {
	if token == "" {
		err = errors.Warning("jwt: parse token failed").WithCause(errors.Warning("token is required"))
		return
	}
	remains, cut := strings.CutPrefix(string(token), "Bearer ")
	if !cut {
		err = errors.Warning("jwt: parse token failed").WithCause(errors.Warning("token is invalid"))
		return
	}
	id, userId, attr, valid, _, parseErr := tokens.core.Parse(remains)
	if parseErr != nil {
		err = errors.Warning("jwt: parse token failed").WithCause(parseErr)
		return
	}
	result = authorizations.ParsedToken{
		Valid:      valid,
		Id:         id,
		UserId:     userId,
		Attributes: attr,
	}
	return
}

func (tokens *jwtTokens) Close() {
	return
}
