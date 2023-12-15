# PPROF
Fns pprof handler.

## Install
```shell
go get github.com/aacfactory/fns-contrib/transports/handlers/pprof
```

## Use
```go
fns.New(fns.Handler(pprof.New()))
```

## Config
```yaml
transport:
  handlers:
    pprof:
      enable: true
```

## URL
```
/debug/pprof/
```
```
/debug/pprof/cmdline
```
```
/debug/pprof/profile
```
```
/debug/pprof/symbol
```
```
/debug/pprof/trace
```