# Websocket for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/http/handlers/websockets
```

## Usage
Make sure tls is used.
```go
app := fns.New(
    fns.Handlers(websockets.Websocket()),
)
```
Setup config
```yaml
http:
  handlers:
    - websockets:
        maxConnections: 1024
        handshakeTimeout: "1s"
        readBufferSize: "4MB"
        writeBufferSize: "4MB"
        enableCompression: false
```
Enable sub protocol handler, such as MQTT.
```go
app := fns.New(
    fns.Handlers(websockets.Websocket(subs...)),
)
```