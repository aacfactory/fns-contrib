# Postgres
Postgres ORM.
## Install
```shell
go get github.com/aacfactory/fns-contrib/databases/postgres
```
## Usage
### Deploy
```go
func dependencies() (v []services.Service) {
  v = []services.Service{
    // add dependencies here
    postgres.New(),
  }
  return
}
```
### Config
See [SQL](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql).
### Register driver
`github.com/lib/pq` and `github.com/jackc/pgx` are all supported.  
Note, when use `pgx`, then don't enable `statements` in config. 
```go
import (
    _ "github.com/lib/pq"
)
```
### Register dialect
Add import in deploy src file.
```go
import (
	_ "github.com/aacfactory/fns-contrib/databases/postgres"
)
```
### Define struct
See [DAC](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/dac).
### Switch package
Use `github.com/aacfactory/fns-contrib/databases/postgres` insteadof `github.com/aacfactory/fns-contrib/databases/sql/dac`.
```go
entry, err = postgres.Insert[Table](ctx, entry) // insteadof dac
```
### Code generator in fn
Add annotation code writer
```go
generates.New(generates.WithAnnotations(postgres.FAG()...))
```
Use `@postgres:transaction` annotation. params are `readonly` and `isolation`.
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
// @postgres:transaction
func some(ctx context.Context, param Param) (result Result, err error) {
	// ...
	return
}
```
Use `@postgres:use` annotation to switch datasource service. param is service name and mark it before `@postgres:transaction`.
```go
// @fn some
// ... some func use transaction
// @postgres:use postgres1
func some(ctx context.Context, param Param) (result Result, err error) {
	// ...
	return
}
```