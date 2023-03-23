# Http3 server for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/http/http3
```

## Usage
Make sure tls is used.
```go
app := fns.New(
    fns.Server(http3.Server()),
)
```

Setup config, see [quic](https://github.com/quic-go/quic-go) for more quic config detail. 
```yaml
http:
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
```
Enable announce that this server supports HTTP/3.
```go
app := fns.New(
    fns.Server(http3.Compatible(&service.FastHttp{})), // any other service.Http 
)
```
```yaml
http:
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
    compatible: # compatible http server config
      foo: "bar" 
```
