# JWT

## usage
Install
```bash
go get github.com/aacfactory/fns-contrib/authorizations/jwts
```
Use jwt
```go
func dependencies() (v []services.Service) {
    v = []services.Service{
        // add dependencies here
        authorizations.New(authorizations.WithTokenEncoder(jwts.New())),
    }
    return
}
```
Setup config
```yaml
authorizations:
  encoder:
    method: "HS256"
    sk: "key"
    issuer: "foo.com"
```
Supported methods:
* HS256, HS384, HS512
* ES256, ES384, ES512
* PS256, PS384, PS512
* RS256, RS384, RS512
* EdDSA

Use Keypair.
```yaml
authorizations:
  encoder:
    method: "RS512"
    publicKey: "path of key"
    privateKey: "path of key"
    issuer: "foo.com"
```