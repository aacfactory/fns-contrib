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
```json
{
  "sql": {
    "masterSlaverMode": false,
    "driver": "",
    "dsn": [
      "username:password@tcp(ip:port)/databases"
    ],
    "maxIdles": 0,
    "maxOpens": 0,
    "enableDebugLog": true,
    "gtmCleanUpSecond": 120,
    "isolation": 2
  }
}
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
sql.BeginTransaction(ctx)
// commit transaction
sql.CommitTransaction(ctx)
// query
sql.Query(ctx, querySQL, ...)
// execute
sql.Execute(ctx, executeSQL, ...)
```


