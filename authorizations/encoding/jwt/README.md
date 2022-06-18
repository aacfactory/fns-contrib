# JWT ENCODING

JWT Authorizations。

## Install

```shell
go get github.com/aacfactory/fns-contrib/authorizations/encoding/jwt
```

## Usage

### Config
```yaml
authorization:
  encoding:
    method: "RS512"
    publicKey: "path of public key"
    privateKey: "path of private key"
    issuer: ""
    audience: 
      - foo
      - bar
    expirations: "720h0m0s"
```

### Register encoding

```go
import (
    _ "github.com/aacfactory/fns-contrib/authorizations/encoding/jwt"
)

```

### Deploy service

```go
app.Deploy(authorizations.Service())
```