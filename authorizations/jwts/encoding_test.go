package jwts_test

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/fns-contrib/authorizations/jwts"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"testing"
	"time"
)

func TestTokenEncoder_Construct(t *testing.T) {
	log, _ := logs.New()
	defer log.Shutdown(context.TODO())

	conf := jwts.Config{
		Method:     "HS256",
		SK:         "key",
		PublicKey:  "",
		PrivateKey: "",
		Issuer:     "foo",
		Audience:   []string{"some"},
	}
	p, _ := json.Marshal(conf)
	config, _ := configures.NewJsonConfig(p)
	comp := jwts.New()
	cErr := comp.Construct(services.Options{
		Id:      "1",
		Version: versions.Version{},
		Log:     log,
		Config:  config,
	})
	if cErr != nil {
		t.Errorf("%+v", cErr)
		return
	}
	encoder := comp.(*jwts.TokenEncoder)

	attrs := make(authorizations.Attributes, 0, 1)
	_ = attrs.Set([]byte("attr"), "attr_value")
	token, encodeErr := encoder.Encode(context.TODO(), authorizations.Authorization{
		Id:         []byte("id"),
		Account:    []byte("account"),
		Attributes: attrs,
		ExpireAT:   time.Now().Add(1 * time.Hour),
	})
	if encodeErr != nil {
		t.Errorf("%+v", encodeErr)
		return
	}
	t.Log(string(token))

	auth, decodeErr := encoder.Decode(context.TODO(), token)
	if decodeErr != nil {
		t.Errorf("%+v", decodeErr)
		return
	}
	t.Log(string(auth.Id), string(auth.Account), auth.ExpireAT)
	for _, attribute := range auth.Attributes {
		t.Log(string(attribute.Key), string(attribute.Value))
	}
}
