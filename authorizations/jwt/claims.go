package jwt

import (
	"fmt"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	gwt "github.com/golang-jwt/jwt/v4"
	"time"
)

func NewUserClaims() *UserClaims {
	return &UserClaims{
		StandardClaims: gwt.StandardClaims{},
		Attr:           json.Object{},
	}
}

type UserClaims struct {
	gwt.StandardClaims
	Attr json.Object `json:"attr,omitempty"`
}

func (c *UserClaims) Valid() (err error) {
	err = c.StandardClaims.Valid()
	if err != nil {
		return
	}

	if !c.Attr.Contains("id") {
		err = fmt.Errorf("id of attr in token was not found")
		return
	}

	return
}

func (c *UserClaims) SetAudience(value string) {
	c.Audience = value
}

func (c *UserClaims) SetExpiresAt(value time.Time) {
	c.ExpiresAt = value.Unix()
}

func (c *UserClaims) SetId(value string) {
	c.Id = value
}

func (c *UserClaims) SetIssuer(value string) {
	c.Issuer = value
}

func (c *UserClaims) SetIssuerAt(value time.Time) {
	c.IssuedAt = value.Unix()
}

func (c *UserClaims) SetNotBefore(value time.Time) {
	c.NotBefore = value.Unix()
}

func (c *UserClaims) SetSub(sub string) {
	c.Subject = sub
}

func (c *UserClaims) SetUser(value fns.User) {
	if value.Attributes() == nil {
		return
	}
	err := c.Attr.UnmarshalJSON(value.Attributes().Raw())
	if err != nil {
		panic(fmt.Sprintf("fns UserClaims SetUser: copy value failed, %v", err))
	}
}

func (c *UserClaims) MapToUserPrincipals(user fns.User) {
	_ = user.Principals().Put("iss", c.Issuer)
	_ = user.Principals().Put("iat", c.IssuedAt)
	_ = user.Principals().Put("sub", c.Subject)
	_ = user.Principals().Put("aud", c.Audience)
	_ = user.Principals().Put("nbf", c.NotBefore)
	_ = user.Principals().Put("exp", c.ExpiresAt)
	_ = user.Principals().Put("jti", c.Id)
}
