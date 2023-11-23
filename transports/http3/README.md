# Http3 server for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/transports/http3
```

## Usage
Make sure tls is used.

```go
import (
    "github.com/aacfactory/fns-contrib/transports/http3"
)
```

```go

tr := http3.New()

```

Setup config, see [quic](https://github.com/quic-go/quic-go) for more quic config detail. 

```yaml
transport:
  name: "http3"
  tls:
    kind: "SSC"
    options:
      ca: "{path of ca}"
      caKey: "{path of ca key}"
  options:
    enableDatagrams: true
    maxRequestHeaderSize: "4K"
    quic:
      handshakeIdleTimeout: "2s"
      maxIdleTimeout: "60s"
      keepAlivePeriod: "60s"
    client:
      maxConnsPerHost: 64
      maxResponseHeaderSize: "4K"
      timeout: "2s"
```

Enable announce that this server supports HTTP/3.

```go

tr := http3.NewWithAlternative(fast.New())

```

```yaml
transport:
  name: "http3"
  tls:
    kind: "SSC"
    options:
      ca: "{path of ca}"
      caKey: "{path of ca key}"
  options:
    enableDatagrams: true
    maxHeaderBytes: "4K"
    quic:
      handshakeIdleTimeout: "2s"
      maxIdleTimeout: "60s"
      keepAlivePeriod: "60s"
    client:
      maxConnsPerHost: 64
      maxResponseHeaderBytes: "4K"
      timeout: "2s"
    alternative: # alternative http server config
      name: "fasthttp" 
      options:
        foo: "bar"
```
