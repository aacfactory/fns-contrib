# Http2 server for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/http/fasthttp2
```

## Usage
Make sure tls is used.
```go
import (
    _ "github.com/aacfactory/fns-contrib/transports/fasthttp2"
)
```
Setup config
```yaml
transport:
  name: "fasthttp2"
  tls:
    kind: "SSC"
    options:
      ca: "{path of ca}"
      caKey: "{path of ca key}"
  options:
    maxRequestBodySize: "4MB" # see fasthttp for more
```

