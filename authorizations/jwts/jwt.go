package jwts

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/golang-jwt/jwt/v4"
	"time"
)

type Claims struct {
	jwt.RegisteredClaims
	Attr *json.Object `json:"attr"`
}

type JWT struct {
	method   jwt.SigningMethod
	pubKey   interface{}
	priKey   interface{}
	issuer   string
	audience []string
}

func (j *JWT) Sign(id string, userId service.RequestUserId, attr *json.Object, expirations time.Duration) (signed string, err error) {
	if id == "" {
		err = errors.Warning("jwt: sign failed").WithCause(errors.Warning("id is required"))
		return
	}
	if !userId.Exist() {
		err = errors.Warning("jwt: sign failed").WithCause(errors.Warning("userId is required"))
		return
	}
	if expirations < 1 {
		err = errors.Warning("jwt: sign failed").WithCause(errors.Warning("expirations is required"))
		return
	}
	if attr == nil {
		attr = json.NewObject()
	}
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    j.issuer,
			Subject:   userId.String(),
			Audience:  j.audience,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(expirations)),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-8 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        id,
		},
		Attr: attr,
	}
	jwtToken := jwt.NewWithClaims(j.method, claims)
	signed, err = jwtToken.SignedString(j.priKey)
	return
}

func (j *JWT) Parse(signed string) (id string, userId service.RequestUserId, attr *json.Object, valid bool, rc jwt.RegisteredClaims, err error) {
	parsed, parseErr := jwt.ParseWithClaims(signed, &Claims{
		RegisteredClaims: jwt.RegisteredClaims{},
		Attr:             json.NewObject(),
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
		userId = service.RequestUserId(claims.Subject)
		attr = claims.Attr
		if attr == nil {
			attr = json.NewObject()
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
