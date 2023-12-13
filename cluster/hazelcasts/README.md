# HAZELCAST

Hazelcast cluster.

## Install
```shell
go get github.com/aacfactory/fns-contrib/cluster/hazelcasts
```

## Use
```go
import (
    _ "github.com/aacfactory/fns-contrib/cluster/hazelcasts"
)
```

## Config
Example:
```yaml
cluster:
  name: "hazelcast"
  option:
    addr:
      - ""
    username: ""
    password: ""
    ssl: 
      enable: false
    keepAlive:
      ttl: "10s"
      interval: "5s"
```

## Use extra shared
When do not want use hazelcast based shared, then use extra, such as redis.

```go
hazelcasts.UseExtraShared(extra)
```
Just use extra store
```go
hazelcasts.UseExtraSharedStore(extra)
```
Just use extra lockers
```go
hazelcasts.UseExtraSharedLockers(extra)
```
Then setup config.
```yaml
cluster:
  option:
    shared: 
```

## Use extra barrier
When do not want use hazelcast based barrier, then use extra, such as redis.
```go
hazelcasts.UseExtraBarrier(extra)

```
```yaml
cluster:
  option:
    barrier: 
```