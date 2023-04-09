# Websocket for FNS

## Install
```bash
go get github.com/aacfactory/fns-contrib/transports/handlers/websockets
```

## Usage
Make sure tls is used.
```go
app := fns.New(
    fns.Transport(fns.TransportOption().Append(websockets.Websocket()))
)
```
Setup config
```yaml
http:
  handlers:
    - websockets:
        maxConnections: 1024
        handshakeTimeout: "1s"
        readTimeout: "10s"
        readBufferSize: "4MB"
        writeTimeout: "60s"
        writeBufferSize: "4MB"
        enableCompression: false
        maxRequestMessageSize: "4KB"
```

Get connection id in function
```go
connId := websockets.ConnectionId(ctx)
```
Send message to client
```go
err := websockets.Send(ctx, connId, payload)
```