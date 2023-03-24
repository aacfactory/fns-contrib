# Http2 server for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/http/http2
```

## Usage
Make sure tls is used.
```go
app := fns.New(
    fns.Server(http2.Server()),
)
```
