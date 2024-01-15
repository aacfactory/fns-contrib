# Mysql
Mysql ORM.
## Install
```shell
go get github.com/aacfactory/fns-contrib/databases/mysql
```
## Usage
### Deploy
```go
app.Deply(mysql.New())
```
### Config
See [SQL](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql).
### Register driver
```go
import (
    _ "github.com/go-sql-driver/mysql"
)
```
### Register dialect
Add import in deploy src file.
```go
import (
	_ "github.com/aacfactory/fns-contrib/databases/mysql"
)
```
### Define struct
See [DAC](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/dac).
### Switch package
Use `github.com/aacfactory/fns-contrib/databases/mysql` insteadof `github.com/aacfactory/fns-contrib/databases/sql/dac`.
```go
entry, err = mysql.Insert[Table](ctx, entry) // insteadof dac
```
### Code generator in fn
Add annotation code writer
```go
generates.New(generates.WithAnnotations(mysql.FAG()...))
```
Use `@mysql:transaction` annotation. params are `readonly` and `isolation`.
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
// @mysql:transaction
func some(ctx context.Context, param Param) (result Result, err error) {
	// ...
	return
}
```
Use `@mysql:use` annotation to switch datasource service. param is service name and mark it before `@mysql:transaction`.
```go
// @fn some
// ... some func use transaction
// @mysql:use mysql1
func some(ctx context.Context, param Param) (result Result, err error) {
	// ...
	return
}
```

## Sequence
See [Sequence](https://github.com/aacfactory/fns-contrib/tree/main/databases/mysql/sequences)

## Note
Virtual column is not fully supported. object and array are same as basic, and select expr of query must be json kind.