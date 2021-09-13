package jwt

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	gwt "github.com/golang-jwt/jwt/v4"
	"time"
)

var (
	prefix = []byte("Bearer ")
)

type Authorizations struct {
	method      gwt.SigningMethod
	pubKey      interface{}
	priKey      interface{}
	issuer      string
	audience    string
	expirations time.Duration
}

func (auth *Authorizations) Encode(ctx fns.Context) (value []byte, err errors.CodeError) {
	userId := ctx.User().Id()
	if userId == "" {
		err = errors.ServiceError("fns UserClaims Encode: sign token failed for user is empty")
		return
	}

	claims := NewUserClaims()
	claims.SetId(fns.UID())
	claims.SetAudience(auth.audience)
	claims.SetExpiresAt(time.Now().Add(auth.expirations))
	claims.SetIssuer(auth.issuer)
	claims.SetIssuerAt(time.Now())
	claims.SetNotBefore(time.Now())
	claims.SetSub(ctx.User().Id())
	copyAttrErr := ctx.User().Attributes().WriteTo(claims.Attr)

	if copyAttrErr != nil {
		err = errors.ServiceError("fns UserClaims Encode: sign token failed for copy user attributes").WithCause(copyAttrErr)
		return
	}

	token := gwt.NewWithClaims(auth.method, claims)

	signed, signErr := token.SignedString(auth.priKey)
	if signErr != nil {
		err = errors.ServiceError("fns UserClaims Encode: sign token failed").WithCause(signErr)
		return
	}

	value = make([]byte, 7+len(signed))
	copy(value[:7], prefix)
	copy(value[7:], signed)
	ctx.User().SetAuthorization(value)
	claims.MapToUserPrincipals(ctx.User())

	return
}

func (auth *Authorizations) Decode(ctx fns.Context, value []byte) (err errors.CodeError) {

	if value == nil || len(value) < 7 {
		err = errors.Unauthorized(fmt.Sprintf("fns JWT Decode: %s is not jwt", string(value)))
		return
	}

	if bytes.Index(value, prefix) != 0 {
		err = errors.Unauthorized(fmt.Sprintf("fns JWT Decode: %s is not jwt", string(value)))
		return
	}

	token, parseErr := gwt.ParseWithClaims(string(value[7:]), NewUserClaims(), func(token *gwt.Token) (interface{}, error) {
		return auth.pubKey, nil
	})

	if parseErr != nil {
		validationError, ok := parseErr.(*gwt.ValidationError)
		if !ok {
			err = errors.Unauthorized(parseErr.Error())
			return
		}
		if validationError.Errors == gwt.ValidationErrorId {
			err = errors.Unauthorized("jti of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorAudience {
			err = errors.Unauthorized("aud of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorExpired {
			err = errors.Unauthorized("exp of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorIssuedAt {
			err = errors.Unauthorized("iat of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorMalformed {
			err = errors.Unauthorized("token is malformed")
		} else if validationError.Errors == gwt.ValidationErrorIssuer {
			err = errors.Unauthorized("iss of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorUnverifiable {
			err = errors.Unauthorized("token could not be verified because of signing problems")
		} else if validationError.Errors == gwt.ValidationErrorClaimsInvalid {
			err = errors.Unauthorized("Generic claims validation error")
		} else if validationError.Errors == gwt.ValidationErrorNotValidYet {
			err = errors.Unauthorized("nbf of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorSignatureInvalid {
			err = errors.Unauthorized("signature validation failed")
		} else {
			err = errors.Unauthorized(validationError.Error())
		}
		return
	}

	claims, ok := token.Claims.(*UserClaims)
	if !ok {
		err = errors.Unauthorized("type of claims is not *jwt.UserClaims")
		return
	}

	claims.MapToUserPrincipals(ctx.User())
	if claims.Attr != nil {
		copyAttrErr := claims.Attr.WriteTo(ctx.User().Attributes())
		if copyAttrErr != nil {
			err = errors.Unauthorized(fmt.Sprintf("fns JWT Decode: %s is invalid, copy user attributes failed", string(value))).WithCause(copyAttrErr)
			return
		}
	}

	return
}
