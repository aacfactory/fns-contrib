package jwts_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/authorizations/jwts"
	"github.com/aacfactory/fns/services/authorizations"
	"testing"
	"time"
)

func TestJWT(t *testing.T) {
	config := jwts.Config{
		Method:     "HS256",
		SK:         "key",
		PublicKey:  "",
		PrivateKey: "",
		Issuer:     "foo",
		Audience:   []string{"a"},
	}
	jwt, jwtErr := config.CreateJWT()
	if jwtErr != nil {
		t.Errorf("%+v", jwtErr)
		return
	}
	attr := make(authorizations.Attributes, 0, 1)
	attr = append(attr, authorizations.Attribute{
		Key:   []byte("a"),
		Value: []byte("\"a\""),
	})
	signed, signErr := jwt.Sign("1", "user:0", attr, time.Now().Add(10*time.Hour))
	if signErr != nil {
		t.Errorf("%+v", signErr)
		return
	}
	fmt.Println(signed)
	fmt.Println(jwt.Parse(signed))
}
