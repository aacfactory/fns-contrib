# JWT ENCODING

JWT Authorizations。

## Install

```shell
go get github.com/aacfactory/fns-contrib/authorizations/encoding/jwt
```

## Usage

### Config

```json
{
  "authorization": {
    "encoding": {
      "method": "",
      // HS256, RS512, SOME VALUE OF ALG
      "sk": "",
      // only HSXXX used
      "publicKey": "",
      // pem file path
      "privateKey": "",
      // pem file path
      "issuer": "",
      "audience": [
        ""
      ],
      "expirations": ""
      // time.Duration Formatter
    }
  }
}
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