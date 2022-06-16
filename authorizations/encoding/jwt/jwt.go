package jwt

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service/builtin/authorizations"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	gwt "github.com/golang-jwt/jwt/v4"
	"strings"
	"time"
)

var (
	prefix = []byte("Bearer ")
)

type Encoding struct {
	log         logs.Logger
	method      gwt.SigningMethod
	pubKey      interface{}
	priKey      interface{}
	issuer      string
	audience    []string
	expirations time.Duration
}

func (encoding *Encoding) Build(options authorizations.TokenEncodingOptions) (err error) {
	encoding.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("jwt: build encoding failed").WithCause(configErr)
		return
	}
	methodName := strings.ToUpper(strings.TrimSpace(config.Method))
	method := gwt.GetSigningMethod(methodName)
	if method == nil {
		err = errors.Warning("jwt: build encoding failed").WithCause(fmt.Errorf("jwt: method is not support"))
		return
	}
	sk := strings.TrimSpace(config.SK)
	pf := strings.TrimSpace(config.PublicKey)
	sf := strings.TrimSpace(config.PrivateKey)
	var pubKey interface{} = nil
	var priKey interface{} = nil
	switch method.(type) {
	case *gwt.SigningMethodHMAC:
		if sk == "" {
			sk = "+-fns"
		}
		pubKey = []byte(sk)
		priKey = []byte(sk)
	case *gwt.SigningMethodECDSA:
		pub, pri, readErr := readPairKeyPem(pf, sf)
		if readErr != nil {
			err = readErr
			return
		}
		var parseErr error
		pubKey, parseErr = gwt.ParseECPublicKeyFromPEM(pub)
		if parseErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(parseErr)
			return
		}
		priKey, parseErr = gwt.ParseECPrivateKeyFromPEM(pri)
		if parseErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(parseErr)
			return
		}
	case *gwt.SigningMethodRSAPSS, *gwt.SigningMethodRSA:
		pub, pri, readErr := readPairKeyPem(pf, sf)
		if readErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(readErr)
			return
		}
		var parseErr error
		pubKey, parseErr = gwt.ParseRSAPublicKeyFromPEM(pub)
		if parseErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(parseErr)
			return
		}
		priKey, parseErr = gwt.ParseRSAPrivateKeyFromPEM(pri)
		if parseErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(parseErr)
			return
		}
	case *gwt.SigningMethodEd25519:
		pub, pri, readErr := readPairKeyPem(pf, sf)
		if readErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(readErr)
			return
		}
		var parseErr error
		pubKey, parseErr = gwt.ParseEdPublicKeyFromPEM(pub)
		if parseErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(parseErr)
			return
		}
		priKey, parseErr = gwt.ParseEdPrivateKeyFromPEM(pri)
		if parseErr != nil {
			err = errors.Warning("jwt: build encoding failed").WithCause(parseErr)
			return
		}
	default:
		err = errors.Warning("jwt: build encoding failed").WithCause(fmt.Errorf("method is not support"))
		return
	}
	issuer := strings.TrimSpace(config.Issuer)
	if issuer == "" {
		issuer = "FNS"
	}
	audience := make([]string, 0, 1)
	if config.Audience != nil {
		audience = config.Audience
	}
	expirations := strings.TrimSpace(config.Expirations)
	if expirations == "" {
		expirations = "720h0m0s"
	}
	expires, expiresErr := time.ParseDuration(expirations)
	if expiresErr != nil {
		err = errors.Warning("jwt: build encoding failed").WithCause(expiresErr)
		return
	}
	encoding.audience = audience
	encoding.method = method
	encoding.issuer = issuer
	encoding.priKey = priKey
	encoding.pubKey = pubKey
	encoding.expirations = expires
	return
}

func (encoding *Encoding) Encode(id string, attributes *json.Object) (token authorizations.Token, err error) {
	if attributes == nil {
		attributes = json.NewObject()
	}
	claims := &Token{
		Claims: &Claims{
			RegisteredClaims: gwt.RegisteredClaims{},
			Attr:             attributes,
		},
		raw: nil,
	}
	claims.Claims.Subject = id
	claims.Claims.ID = uid.UID()
	claims.Claims.Issuer = encoding.issuer
	claims.Claims.IssuedAt = gwt.NewNumericDate(time.Now())
	claims.Claims.Audience = encoding.audience
	claims.Claims.ExpiresAt = gwt.NewNumericDate(time.Now().Add(encoding.expirations))
	claims.Claims.NotBefore = gwt.NewNumericDate(time.Now().Add(-8 * time.Hour))
	jwtToken := gwt.NewWithClaims(encoding.method, claims.Claims)
	signed, signErr := jwtToken.SignedString(encoding.priKey)
	if signErr != nil {
		err = errors.ServiceError("jwt: sign token failed").WithCause(signErr)
		return
	}

	raw := make([]byte, 7+len(signed))
	copy(raw[:7], prefix)
	copy(raw[7:], signed)
	claims.raw = raw
	token = claims
	return
}

func (encoding *Encoding) Decode(p []byte) (token authorizations.Token, err error) {
	if p == nil || len(p) < 7 {
		err = errors.Unauthorized(fmt.Sprintf("jwt: decode failed for %s is not jwt", string(p)))
		return
	}
	if bytes.Index(p, prefix) != 0 {
		err = errors.Unauthorized(fmt.Sprintf("jwt: decode failed for %s is not jwt", string(p)))
		return
	}
	jwtClaims, parseErr := gwt.ParseWithClaims(string(p[7:]), &Claims{
		RegisteredClaims: gwt.RegisteredClaims{},
		Attr:             json.NewObject(),
	}, func(token *gwt.Token) (interface{}, error) {
		return encoding.pubKey, nil
	})
	if parseErr != nil {
		validationError, ok := parseErr.(*gwt.ValidationError)
		if !ok {
			err = errors.Unauthorized("jwt: parse token failed").WithCause(parseErr)
			return
		}
		if validationError.Errors == gwt.ValidationErrorId {
			err = errors.Unauthorized("jwt: jti of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorAudience {
			err = errors.Unauthorized("jwt: aud of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorExpired {
			err = errors.Unauthorized("jwt: exp of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorIssuedAt {
			err = errors.Unauthorized("jwt: iat of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorMalformed {
			err = errors.Unauthorized("jwt: token is malformed")
		} else if validationError.Errors == gwt.ValidationErrorIssuer {
			err = errors.Unauthorized("jwt: iss of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorUnverifiable {
			err = errors.Unauthorized("jwt: token could not be verified because of signing problems")
		} else if validationError.Errors == gwt.ValidationErrorClaimsInvalid {
			err = errors.Unauthorized("jwt: generic claims validation error")
		} else if validationError.Errors == gwt.ValidationErrorNotValidYet {
			err = errors.Unauthorized("jwt: nbf of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorSignatureInvalid {
			err = errors.Unauthorized("jwt: signature validation failed")
		} else {
			err = errors.Unauthorized("jwt: parse token failed").WithCause(parseErr)
		}
		return
	}
	claims, ok := jwtClaims.Claims.(*Claims)
	if !ok {
		err = errors.Unauthorized("jwt: decode failed for type of claims is not *jwt.Claims")
		return
	}
	token = &Token{
		Claims: claims,
		raw:    p,
	}
	return
}
