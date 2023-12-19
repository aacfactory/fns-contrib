# Limiter

Request limiter

## Install
```shell
go get github.com/aacfactory/fns-contrib/transports/middlewares/limiter
```

## Config
```yaml
transport:
  middlewares:
    limiter:
      enable: true
      everySeconds: 10
      burst: 0
      device:
        enable: true
        everySeconds: 10
        burst: 10
        cacheSize: 4096
```

## Usage
```go
fns.New(fns.Middleware(limiter.New()))
```
With alarm.
```go
fns.New(fns.Middleware(limiter.New(limiter.WithAlarm(alarm))))
```
```yaml
limiter:
  alarm:
    everySeconds: 10
    burst: 10
    options:
      ...
```