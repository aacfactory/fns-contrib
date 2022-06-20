# Http3 server for FNS

## Usage
Make sure tls is used.
```go
app := fns.New(
    fns.Server(http3.Server()),
)
```
Cluster mode
```go
app := fns.New(
    fns.Server(http3.Server()),
    fns.ClusterClientBuilder(http3.ClientBuild),
)
```
