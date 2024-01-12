package jwts

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/services/authorizations"
	"github.com/aacfactory/json"
	"github.com/golang-jwt/jwt/v5"
	"time"
)

type Claims struct {
	jwt.RegisteredClaims
	Attr map[string]json.RawMessage `json:"attr"`
}

type JWT struct {
	method   jwt.SigningMethod
	pubKey   any
	priKey   any
	issuer   string
	audience []string
}

func (j *JWT) Sign(tid string, account string, attributes authorizations.Attributes, expireAT time.Time) (signed string, err error) {
	if tid == "" {
		err = errors.Warning("jwt: sign failed").WithCause(errors.Warning("token id is required"))
		return
	}
	if account == "" {
		err = errors.Warning("jwt: sign failed").WithCause(errors.Warning("authorization account is required"))
		return
	}
	if expireAT.IsZero() {
		err = errors.Warning("jwt: sign failed").WithCause(errors.Warning("expirations is required"))
		return
	}
	attr := make(map[string]json.RawMessage)
	for _, attribute := range attributes {
		attr[bytex.ToString(attribute.Key)] = attribute.Value
	}
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    j.issuer,
			Subject:   account,
			Audience:  j.audience,
			ExpiresAt: jwt.NewNumericDate(expireAT),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-8 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        tid,
		},
		Attr: attr,
	}
	jwtToken := jwt.NewWithClaims(j.method, claims)
	signed, err = jwtToken.SignedString(j.priKey)
	return
}

func (j *JWT) Parse(signed string) (id string, account string, attributes authorizations.Attributes, valid bool, rc jwt.RegisteredClaims, err error) {
	parsed, parseErr := jwt.ParseWithClaims(signed, &Claims{
		RegisteredClaims: jwt.RegisteredClaims{},
		Attr:             make(map[string]json.RawMessage),
	}, func(token *jwt.Token) (interface{}, error) {
		return j.pubKey, nil
	}, jwt.WithValidMethods([]string{j.method.Alg()}))
	if parsed != nil {
		claims, isClaims := parsed.Claims.(*Claims)
		if !isClaims {
			err = errors.Warning("jwt: parse failed").WithCause(errors.Warning("claims is not matched")).WithMeta("token", signed)
			return
		}
		id = claims.ID
		account = claims.Subject
		for key, message := range claims.Attr {
			attributes = append(attributes, authorizations.Attribute{
				Key:   bytex.FromString(key),
				Value: message,
			})
		}
		valid = parsed.Valid
		rc = claims.RegisteredClaims
		return
	}
	if parseErr != nil {
		err = errors.Warning("jwt: parse failed").WithCause(parseErr).WithMeta("token", signed)
	} else {
		err = errors.Warning("jwt: parse failed").WithCause(errors.Warning("no parsed token and error"))
	}
	return
}
