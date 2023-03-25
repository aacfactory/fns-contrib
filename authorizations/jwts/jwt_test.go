package jwts_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/authorizations/jwts"
	"github.com/aacfactory/json"
	"testing"
)

func TestJWT(t *testing.T) {
	config := jwts.Config{
		Method:      "HS256",
		SK:          "key",
		PublicKey:   "",
		PrivateKey:  "",
		Issuer:      "foo",
		Audience:    []string{"a"},
		Expirations: "1s",
	}
	jwt, jwtErr := config.CreateJWT()
	if jwtErr != nil {
		t.Errorf("%+v", jwtErr)
		return
	}
	signed, signErr := jwt.Sign("1", "user:0", json.NewObject())
	if signErr != nil {
		t.Errorf("%+v", signErr)
		return
	}
	fmt.Println(signed)
	fmt.Println(jwt.Parse(signed))
}
