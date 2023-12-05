# SQL

One sql service for fns.

## Features
* Global transaction
* Support prepared statement
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
  dialect: "postgres"
  isolation: 2
  transactionMaxAge: 10
  debugLog: true
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
  dialect: "postgres"
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
  dialect: "postgres"
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

Isolation:
* Default: 0
* ReadUncommitted: 1
* ReadCommitted: 2
* WriteCommitted: 3
* RepeatableRead: 4
* Snapshot: 5
* Serializable: 6
* Linearizable: 7

### Import driver
```go
import _ "github.com/go-sql-driver/mysql"
```

### Deploy
```go
app.Deply(sql.Service())
```

### Proxy usage
```go
// begin transaction 
sql.Begin(ctx)
// commit transaction
sql.Commit(ctx)
// rollback transaction
sql.Rollback(ctx)
// query
sql.Query(ctx, querySQL, ...)
// execute
sql.Execute(ctx, executeSQL, ...)
```

### Code generator in fn
Use `@sql:transaction` annotation. params are `readonly` and `isolation`.
* readonly: set the transaction to be readonly.
* isolation: use spec isolation. default is use isolation of config.
  * ReadCommitted
  * ReadUncommitted
  * WriteCommitted
  * RepeatableRead
  * Snapshot
  * Serializable
  * Linearizable
```go
// @fn some
// ... some func use transaction
// @sql:transaction
func some(ctx context.Context, param Param) (result Result, err error) {
	// ...
	return
}
```
Use `@sql:use` annotation to switch datasource service. param is service name.
```go
// @fn some
// ... some func use transaction
// @sql:use postgres1
func some(ctx context.Context, param Param) (result Result, err error) {
	// ...
	return
}
```

### ORM
* [POSTGRES](https://github.com/aacfactory/fns-contrib/tree/main/databases/postgres)
* [MYSQL](https://github.com/aacfactory/fns-contrib/tree/main/databases/mysql)
* [DAC](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/dac)

### Multi sources

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
sql.Query(sql.Use(ctx, "postgres1"), querySQL, ...)
sql.Query(sql.Use(ctx, "mysql1"), querySQL, ...)
```