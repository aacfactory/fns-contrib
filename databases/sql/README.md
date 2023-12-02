# SQL

One sql service for fns.

## Features
* Global transaction
* Proxy
* Support master slaver kind
* Support cluster kind
## Install

```shell
go get github.com/aacfactory/fns-contrib/databases/sql
```

## Usage

### Config

Standalone:
```yaml
sql:
  kind: "standalone"
  isolation: 2
  transactionMaxAge: 10
  options:
    driver: "postgres"
    dsn: "username:password@tcp(ip:port)/databases"
    maxIdles: 0
    maxOpens: 0
    statements:
      enable: true
      cacheSize: 256
      evictTimeoutSeconds: 10
```
MasterSlave:
```yaml
sql:
  kind: "masterSlave"
  isolation: 2
  transactionMaxAge: 10
  options:
    driver: "postgres"
    master: "username:password@tcp(ip:port)/databases"
    slavers:
      - "username:password@tcp(ip:port)/databases"
      - "username:password@tcp(ip:port)/databases"
    maxIdles: 0
    maxOpens: 0
    statements:
      enable: true
      cacheSize: 256
      evictTimeoutSeconds: 10
```
Cluster:
```yaml
sql:
  kind: "cluster"
  isolation: 2
  transactionMaxAge: 10
  options:
    driver: "postgres"
    dsn:
      - "username:password@tcp(ip:port)/databases"
      - "username:password@tcp(ip:port)/databases"
    maxIdles: 0
    maxOpens: 0
    statements:
      enable: true
      cacheSize: 256
      evictTimeoutSeconds: 10
```
Note: when use some driver like `pgx`, then disable statements, cause driver has handled statements.
### Import driver
```go
import _ "github.com/go-sql-driver/mysql"
```

### Deploy
```go
app.Deply(sql.Service())
```

### Proxy usage
See [proxy.go](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/proxy.go)
```go
// begin transaction 
sql.Begin(ctx)
// commit transaction
sql.Commit(ctx)
// query
sql.Query(ctx, querySQL, ...)
// execute
sql.Execute(ctx, executeSQL, ...)
```
### ORM
* [postgres](https://github.com/aacfactory/fns-contrib/tree/main/databases/postgres)
* [mysql](https://github.com/aacfactory/fns-contrib/tree/main/databases/mysql)
* [common](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/dac)

### Multi database source

use multi database service to implements

Config:
```yaml
postgres1:
  kind: "standalone"

mysql1:
  kind: "standalone"
```
Deploy:
```yaml
app.Deploy(sql.Service(sql.WithName("postgres1")))
app.Deploy(sql.Service(sql.WithName("mysql1")))
```
Proxy
```go
sql.Query(sql.EndpointName(ctx, "postgres1"), querySQL, ...)
sql.Query(sql.EndpointName(ctx, "mysql1"), querySQL, ...)
```