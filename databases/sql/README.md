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

* Standalone type
    * masterSlaverMode = false
    * dsn size is one
* Master slaver type
    * masterSlaverMode = true
    * first of dsn is master, afters are slavers
* Cluster type
    * masterSlaverMode = false
    * all in dsn is members

Example:
```yaml
sql:
  kind: "standalone"
  isolation: 2
  transactionMaxAge: 10
  options:
    dsn: "username:password@tcp(ip:port)/databases"
    maxIdles: 0
    maxOpens: 0
```

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
### ORM usage
* [database access layer](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/dal)

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