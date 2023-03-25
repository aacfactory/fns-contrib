# JWT

## usage
Install
```bash
go get github.com/aacfactory/fns-contrib/authorizations/jwts
```
Use jwt
```go
app.Deploy(authorizations.Service(jwts.Tokens()))
```
Setup config
```yaml
authorizations:
  jwt:
    method: "HS256"
    sk: "key"
    issuer: "foo.com"
    expirations: "312h0m0s"
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
  jwt:
    method: "RS512"
    publicKey: "path of key"
    privateKey: "path of key"
    issuer: "foo.com"
    expirations: "312h0m0s"
```