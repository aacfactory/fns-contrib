package jwts

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/authorizations"
)

type Tokens struct {
}

func (tokens *Tokens) Name() (name string) {
	name = "jwt"
	return
}

func (tokens *Tokens) Build(options service.ComponentOptions) (err error) {
	//TODO implement me
	panic("implement me")
}

func (tokens *Tokens) Close() {
	//TODO implement me
	panic("implement me")
}

func (tokens *Tokens) Create(ctx context.Context, param authorizations.CreateTokenParam) (token authorizations.Token, err errors.CodeError) {
	//TODO implement me
	panic("implement me")
}

func (tokens *Tokens) Verify(ctx context.Context, token authorizations.Token) (result authorizations.VerifyResult, err errors.CodeError) {
	//TODO implement me
	panic("implement me")
}
