# Standard http server for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/transports/standard
```

## Usage
```go
import (
    _ "github.com/aacfactory/fns-contrib/transports/standard"
)

```
Setup config
```yaml
transport:
  name: "http"
  tls:
    kind: "SSC"
    options:
      ca: "{path of ca}"
      caKey: "{path of ca key}"
  options:
    maxRequestHeaderSize: "4K"
    maxRequestBodySize: "4MB"
    readTimeout: "10s"
    readHeaderTimeout: "5s"
    writeTimeout: "30s"
    idleTimeout: "90s" 
    client:
      maxConnsPerHost: 64
      maxResponseHeaderSize: "4K"
      timeout: "30s"
      disableKeepAlive: false
      maxIdleConnsPerHost: 100
      idleConnTimeout: "90s"
      tlsHandshakeTimeout: "10s"
      expectContinueTimeout: "1s"
```
